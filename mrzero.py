"""
Goals:

    1)  Specify one container with > 1000 objects and
        have the zerovm application reduce to 1.

    2)  Specify a string which is a container name
        prefix (string), and have the zerovm application
        run against all of those containers to create one result.

    3)  Do fun/automated things with zerovm app manifest templating.

    # Run job defined in container X (mapper.py, reducer.py)
    #   on objects in container(s) [a, b, c]
    # OR run job defined in local files (mapper.py, reducer.py)
    #   on "objects" in directories [a/, b/, and c/]
    #   which would upload and execute accordinly

"""
import copy
import itertools
import logging
import math
try:
    import simplejson as json
except ImportError:
    import json
import os
import sys
import threading
import urlparse

from multiprocessing import pool

import swiftclient

LOG = logging.getLogger(__name__)

CACHE = {}
PER_JOB = 10

# paths
SW = "swift://./"
RESULTS = "%s{jobtainer}/results/results-{job_num}.json" % SW
ERRORS = "%s{jobtainer}/errors/{object_ref}.err" % SW
MAPPER = "%s{jobtainer}/mapper.py" % SW
REDUCER = "%s{jobtainer}/reducer.py" % SW


def cached(original_func):
    """Cache return values."""
    ih = lambda o: getattr(o, '__hash__', None)
    def new_func(*args, **kw):
        blob = (original_func.__class__.__name__, original_func.__name__)
        blob += tuple((k for k in sorted(args) if ih(k)))
        blob += tuple(((k, v) for k, v in sorted(kw.items()) if ih(v)))
        seek = hash(blob)
        if seek not in CACHE:
            CACHE[seek] = original_func(*args, **kw)
        return copy.deepcopy(CACHE[seek])
    new_func.__doc__ = original_func.__doc__
    return new_func

def normalized_path(path, must_exist=True):
    """Normalize and expand a shorthand or relative path.

    If the value is Falsy, a non-string, or invalid, raise ValueError.
    """
    if not path or not isinstance(path, basestring):
        raise ValueError("The directory or path should be a string.")
    norm = os.path.normpath(path)
    norm = os.path.abspath(os.path.expanduser(norm))
    if must_exist:
        if not os.path.exists(norm):
            raise ValueError("%s is not a valid path." % norm)
    return norm

def upload_jobtainer(directory, client, dryrun=False):
    """Upload scripts in specified dir to jobtainer of the same name.

    Local directory specified should contain 2 files: mapper.py, reducer.py
    """
    normdir = normalized_path(directory)
    jobtainer_name = os.path.split(normdir)[1]
    for fname in ('mapper.py', 'reducer.py'):
        filepath = os.path.join(normdir, fname)
        if not os.path.exists(filepath):
            msg = ('%s not found in %s. A jobtainer should contain a '
                   'mapper.py and a reducer.py' % (fname, directory))
            raise IOError(msg)
        else:
            if dryrun:
                LOG.debug("Dry-run. Would otherwise upload %s.",
                          filepath)
                continue
            client.put_container(jobtainer_name)
            with open(filepath, 'r') as script:
                client.put_object(jobtainer_name, fname, script.read())

class ZMapReduce(object):

    def __init__(self, jobtainer, inputs=None, per_job=160, **client):

        # properties
        self._job_spec = None
        self.manifests = None
        self.final_result = None
        self.results = None
        self.per_job = per_job

        self._client_kwargs = client
        self.client = get_client(**client)

        all_containers = self.list_containers(select='name')
        if jobtainer not in all_containers:
            raise ValueError("Container '%s' does not exist." % jobtainer)
        else:
            self.jobtainer = jobtainer
            if not all(k in self.list_objects(self.jobtainer, select='name')
                       for k in ('mapper.py', 'reducer.py')):
                raise ValueError("Jobtainer should have a mapper.py and a "
                                 "reducer.py")

        if not inputs:
            input_containers = self.list_containers(select='name')
            input_containers.remove(self.jobtainer)
        else:
            input_containers = []
            if isinstance(inputs, list):
                for substr in inputs:
                    input_containers += self.get_input_containers(prefix=substr)
            elif isinstance(inputs, basestring):
                input_containers += self.get_input_containers(prefix=inputs)
            else:
                raise ValueError("'inputs' is a str or list of strings "
                                 "matching input containers.")
        if not input_containers:
            raise ValueError("No input containers matched %s" % inputs)
        self.input_containers = input_containers
        self.all_the_objects = self.list_all_objects_for_job()

    @property
    def job_spec(self):
        if not self._job_spec:
            self._job_spec = calculate_job_spec(
                len(self.all_the_objects), self.per_job)
        return self._job_spec

    def get_input_containers(self, prefix=None):

        cntrs = self.list_containers(select='name')
        if prefix:
            baddies = [x for x in cntrs if not x.startswith(prefix)]
            for cname in baddies:
                cntrs.remove(cname)
        return cntrs

    def list_all_objects_for_job(self):

        all_the_objects = []
        for c in self.input_containers:
            each = ("%s/%s" % (c, n)
                    for n in self.list_objects(c, select='name'))
            map(lambda x: all_the_objects.append(x), each)
        return all_the_objects

    def run_jobs(self):
        if not self.manifests:
            self.generate_manifests()

        self.cleanup()

        results = []
        for tier, manis in sorted(self.manifests.items()):
            objs = sum([len(k) - 1 for k in manis])  # minus reducer node
            print ("Running %s zerovm jobs concurrently across %s "
                   "objects (%s/job) for job tier %s"
                   % (len(manis), objs, self.per_job, tier))
            result = execute(*manis, **self._client_kwargs)
            results.append((tier, result))
        self.results = dict(results)
        self.get_final_result()
        return self.results

    def generate_manifests(self):

        # would probably be reasonable to define a mutable
        # ... an instance attribute which kept track
        # of the tiers, their manifests, and results,
        # which could be easily referenced in the following loop

        self.manifests = {n + 1: []
                          for n in xrange(self.job_spec['total_tiers'])}

        rresults = self.all_the_objects
        i = 0
        for tier, execution_group in self.manifests.items():
            breakup = itertools.izip_longest(
                *(iter(rresults),) * self.per_job)
            for jobjects in breakup:
                job_num = "%s-%s" % (tier, i)
                #  input is previous tier
                #  resultgroup is directory from jobjects
                execution_group.append(
                    self.mapreduce_manifest(job_num, jobjects))
                i += 1
            rresults = [d['path'] for g in execution_group
                        for n in g if n['name'] == 'reducer'
                        for d in n['devices'] if d['name'] == 'stdout']
            assert len(execution_group) == self.job_spec['reduces'][tier]

        assert len(rresults) == 1
        frpath = urlparse.urlparse(rresults[0]).path
        fl = [k for k in frpath.split('/') if k]
        self.final_result = {
            'swift_url': rresults[0],
            'ref': "/".join(fl[1:]),
            'container': fl[0],
            'object_path': frpath
        }
        assert i == sum(self.job_spec['reduces'][1:])

    def get_final_result(self):
        if not self.final_result:
            return
        if not self.results:
            return
        if 'value' not in self.final_result:
            headers, self.final_result['value'] = self.client.get_object(
                self.final_result['container'],
                self.final_result['ref'])
            try:
                self.final_result['value'] = json.loads(self.final_result['value'])
            except ValueError:
                pass

        return self.final_result['value']

    def cleanup(self):
        """Cleanup errors/results for job.

        Try to do this asynchronously.
        """

        deleteasync = lambda x: threading.Thread(
            target=self.client.delete_object, args=(self.jobtainer, x))
        tasks = []
        for r in self.list_objects(self.jobtainer, select='name'):
            if r.startswith('errors'):
                t = deleteasync(r)
                t.start()
                tasks.append(t)
            elif r.startswith('results'):
                t = deleteasync(r)
                t.start()
                tasks.append(t)
            elif r.endswith(".err"):
                t = deleteasync(r)
                t.start()
                tasks.append(t)
            elif r.endswith(".pyc"):
                t = deleteasync(r)
                t.start()
                tasks.append(t)
        objects_to_remove = len(tasks)
        while tasks:
            i = 1
            for task in tasks:
                task.join()
                sys.stdout.write('.')
                sys.stdout.flush()
                tasks.remove(task)
        if objects_to_remove:
            print ("\nFinished cleaning up [%s object(s)] "
                   "from previous run." % (objects_to_remove))

    def mapreduce_manifest(self, job_num, objects):

        def _mapper_manifest(job_num, object_ref):
            if not object_ref:
                return
            object_num = objects.index(object_ref)
            if object_ref.startswith(SW):
                object_ref = object_ref[len(SW):]
            mapper_node = {"name": ("mapper-%s-%s"
                                    % (job_num, object_num)),
                           "exec": {"path": "file://python:python"}}
            mapper_connect = ["reducer"]
            mapper_devices = [
                {"name": "stdin",
                 "path": "%s" % MAPPER.format(jobtainer=self.jobtainer)},
                {"name": "input",
                 "path": "%s%s" % (SW, object_ref)},
                {"name": "stderr",
                 "path": ("%s" % ERRORS.format(
                                jobtainer=self.jobtainer,
                                object_ref=os.path.split(object_ref)[-1])),
                 "content_type": "text/plain"},
                {"name": "python"}
            ]
            mapper_node['devices'] = mapper_devices
            mapper_node['connect'] = mapper_connect
            return mapper_node

        mapper_manifest = (
            lambda obr: _mapper_manifest(job_num, obr))
        nodes = map(mapper_manifest, objects)
        nodes = [n for n in nodes if n]

        reducer_node = self.reducer_manifest(job_num)
        nodes.append(reducer_node)

        return nodes

    def reducer_manifest(self, job_num, explicit_input=None):
        reducer_node = {"name": "reducer",
                        "exec": {"path": "file://python:python"}}
        reducer_devices = [
            {"name": "stdin",
             "path": "%s" % REDUCER.format(jobtainer=self.jobtainer)},
            {"name": "stdout",
             "path": "%s" % RESULTS.format(jobtainer=self.jobtainer,
                                           job_num=job_num),
             "content_type": "application/json"},
            {"name": "python"},
            {"name": "stderr",
             "path": "%s%s/reducer.err" % (SW, self.jobtainer),
             "content_type": "text/plain"}
        ]
        if explicit_input:
            reducer_devices.append(
                {"name": "input",
                 "path": explicit_input})
        reducer_node['devices'] = reducer_devices
        return reducer_node

    @cached
    def list_objects(self, container_name, select=None,
                     with_headers=False):

        if not container_name:
            return []
        headers, objects = self.client.get_container(
            container_name, full_listing=True)
        if select:
            objects = [k[select] for k in objects if select in k]
        if with_headers:
            return headers, objects
        return objects

    @cached
    def list_containers(self, select=None, with_headers=False):

        headers, containers = self.client.get_account(full_listing=True)
        if select:
            containers = [k[select] for k in containers if select in k]
        if with_headers:
            return headers, containers
        return containers


def make_json(manifest):
    """Return a json-encoded string from a file, path, or dict."""
    if isinstance(manifest, file):
        manifest = manifest.read()
    elif isinstance(manifest, (list, dict)):
        manifest = json.dumps(manifest)
    elif os.path.exists(manifest):
        with open(manifest, 'r') as man:
            manifest = man.read()
    return manifest


@cached
def get_client(auth=None, user=None, key=None, **kwargs):
    """Return a client using v1 auth.

    Uses supplied keyword arguments, env vars, or a combination of both.
    """
    if not all((auth, user, key)):
        eauth, euser, ekey = map(os.getenv, ('ST_AUTH', 'ST_USER', 'ST_KEY'))
        auth, user, key = auth or eauth, user or euser, key or ekey

    # ZeroCloudConnection inherts from swiftclient.Connection
    # includes many kwarg/params... may be worth looking into
    client = swiftclient.Connection(auth, user, key, **kwargs)
    client.url, client.token = client.get_auth()
    return client


# yanked half a fn from zpm
def _post_job(url, token, json_data, http_conn=None, response_dict=None):
    # Modelled after swiftclient.client.post_account.
    headers = {'X-Auth-Token': token,
               'Accept': 'application/json',
               'X-Zerovm-Execute': '1.0',
               'Content-Type': 'application/json'}

    if http_conn:
        parsed, conn = http_conn
    else:
        parsed, conn = swiftclient.http_connection(url)

    return conn.request('POST', parsed.path, json_data, headers)


def _execute(job, retries=5, **clientkwargs):
    """Target for task pool."""
    try:
        # client unpickleable, need to "re-fetch" client
        # hint: its @cached
        client = get_client(**clientkwargs)
        job = make_json(job)
        response_dict = {}
        response = client._retry(
            None, _post_job, job,
            response_dict=response_dict)
        response.raise_for_status()
        return response
    except Exception as err:
        retries -= 1
        if retries > 0:
            print "Retrying on exception | %s" % str(err)
            return _execute(job, retries=retries, **clientkwargs)
        else:
            print "Max retries met. Fail."
            return err


def execute(*manifests, **clientkwargs):
    """Execute zerovm app given one or more manifest definitions.

    Return http (requests) response object(s).
    """
    #result = map(_execute, manifests)
    #return result
    ppool = pool.ThreadPool()
    result = ppool.map(_execute, manifests)
    return result

@cached
def calculate_job_spec(total_objects, per_job):
    """Determine job spec from total objects and max objects per job."""

    definition = {'total_objects': total_objects,
                  'per_job': per_job}
    fn = lambda tot, per: (int(math.ceil(
        float(tot)/float(per))))
    definition['mapper_jobs'] = fn(
        total_objects, per_job)
    tiers = int(math.ceil(math.log(total_objects, per_job)))
    definition['total_tiers'] = tiers

    # reducing...
    x = total_objects
    reduces = [total_objects]
    while x != 1:
        x = fn(x, per_job)
        reduces.append(x)
    definition['reduces'] = reduces
    definition['final_job_objects'] = reduces[-2]
    return definition


def allattrs(thing, recurse=2):
    if not recurse:
        return thing
    try:
        d = vars(thing)
    except TypeError:
        d = {}
    d.update(getattr(thing, '__dict__', {}))
    for att in dir(thing):
        if not att.startswith('_'):
            d[att] = allattrs(getattr(thing, att, None), recurse-1)
    return d
