# import eventlet
# eventlet.monkey_patch()
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
import time
try:
    import simplejson as json
except ImportError:
    import json
import os
import urlparse

from concurrent import futures
import requests
import sortedcontainers
import swiftclient
import wrapt

LOG = logging.getLogger(__name__)
CPU_COUNT = futures.process.multiprocessing.cpu_count()

CACHE = {}
# number of objects per job should probably be either:
#   - less than 120 (hits http payload limit around here)
#   OR
#   - greater than or equal to the number of objects in your
#     input containers. the module will determine if this
#     is the case and use * globbing to select all of the
#     objects in the container.

PER_JOB = 100
# zebra runs 20 nodes
CONCURRENT_JOBS = 15

# paths
SW = "swift://./"
RESULTS = "%s{jobtainer}/results/results-{job_num}.json" % SW
ERRORS = "%s{jobtainer}/errors/{object_ref}.err" % SW
MAPPER = "%s{jobtainer}/mapper.py" % SW
REDUCER = "%s{jobtainer}/reducer.py" % SW
NULLMAPPER = "%s{jobtainer}/nullmapper.py" % SW


@wrapt.decorator
def cached(original_func, instance, args, kw):
    """Memoize plz."""
    ih = lambda o: callable(getattr(o, '__hash__', None))
#    def new_func(*args, **kw):
    blob = (original_func.__class__.__name__, original_func.__name__)
    blob += tuple((k for k in sorted(args) if ih(k)))
    blob += tuple(((k, v) for k, v in sorted(kw.items()) if ih(v)))
    seek = hash(blob)
    if seek not in CACHE:
        CACHE[seek] = original_func(*args, **kw)
        LOG.debug("Caching return value from %s", original_func.__name__)
    else:
        LOG.debug("Returning %s from cache.", original_func.__name__)
    return copy.deepcopy(CACHE[seek])
#    return new_func


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
                LOG.debug("Dry-run. Would otherwise upload '%s' to "
                          "container '%s' with object name '%s'",
                          os.path.relpath(filepath), jobtainer_name, fname)
                continue
            try:
                client.get_container(jobtainer_name)
            except swiftclient.ClientException:
                client.put_container(jobtainer_name)
            with open(filepath, 'r') as script:
                client.put_object(jobtainer_name, fname, script.read())


class ZMapReduce(object):

    def __init__(self, jobtainer, inputs=None, per_job=PER_JOB,
                 client=None, **client_kwargs):

        # properties
        self._job_spec = None
        self.manifests = sortedcontainers.SortedDict()
        self.final_result = None
        self.results = None
        self.per_job = per_job

        if not client:
            self._client_kwargs = client_kwargs
            self.client = get_client(**client_kwargs)
        else:
            if not isinstance(client, swiftclient.Connection):
                raise ValueError("'client' should be a "
                                 "swiftclient.Connection instance")
            else:
                self.client = setup_client(client)
                # these attributes are set in order to create "new"
                # swiftclient connections when multithreading
                self._client_kwargs = {
                    'auth': self.client.authurl,
                    'user': self.client.user,
                    'key': self.client.key,
                }

        all_containers = self.list_containers(select='name')
        if jobtainer not in all_containers:
            raise ValueError("Container '%s' does not exist." % jobtainer)
        else:
            self.jobtainer = jobtainer
            if not all(k in self.list_objects(self.jobtainer, select='name')
                       for k in ('mapper.py', 'reducer.py')):
                raise ValueError("Jobtainer should have a mapper.py and a "
                                 "reducer.py")

        nmupload = futures.ThreadPoolExecutor(1).submit(
            self.upload_nullmapper)

        if not inputs:
            input_containers = self.list_containers(select='name')
            input_containers.remove(self.jobtainer)
        else:
            input_containers = []
            if isinstance(inputs, list):
                for substr in inputs:
                    containers = self.get_input_containers(prefix=substr)
                    input_containers += containers
            elif isinstance(inputs, basestring):
                input_containers += self.get_input_containers(prefix=inputs)
            else:
                raise ValueError("'inputs' is a str or list of strings "
                                 "matching input containers.")
        if not input_containers:
            raise ValueError("No input containers matched %s" % inputs)
        self.input_containers = input_containers
        self.all_the_objects = self.list_all_objects_for_job()

        # make sure the nullmapper upload finished
        assert nmupload.result(timeout=.1)

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

        list_objects = lambda c: ["%s/%s" % (c, n) for n in
                                  self.list_objects(c, select='name')]
        with futures.ThreadPoolExecutor(CPU_COUNT*16) as pool:
            result = pool.map(list_objects, self.input_containers)
            all_the_objects = list(itertools.chain.from_iterable(result))
        return all_the_objects

    def __call__(self):
        if not self.manifests:
            self.generate_manifests()

        self.cleanup()

        results = []
        assert isinstance(self.manifests, sortedcontainers.SortedDict)
        for tier, manis in self.manifests.items():
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

        self.manifests.update(
            {str(n + 1): []
             for n in xrange(self.job_spec['total_tiers'])})

        rresults = self.all_the_objects
        i = 0
        assert isinstance(self.manifests, sortedcontainers.SortedDict)
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
            assert len(execution_group) == self.job_spec['reduces'][int(tier)]

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
                value = self.final_result['value']
                self.final_result['value'] = json.loads(value)
            except ValueError:
                pass

        return self.final_result['value']

    def upload_nullmapper(self):
        """Upload nullmapper.py."""
        target = NULLMAPPER.format(jobtainer=self.jobtainer)
        fname = os.path.split(target)[-1]
        assert os.path.join(self.jobtainer, fname) in target
        with open(fname, 'r') as script:
            return self.client.put_object(self.jobtainer, fname,
                                          script.read())

    def cleanup(self, timeout=45):
        """Cleanup errors/results for job.

        Try to do this asynchronously.
        """
        def should_cleanup(name):
            """Determine whether the object should be deleted."""
            sw = lambda substr: name.startswith(substr)
            ew = lambda substr: name.endswith(substr)
            return any((sw('errors'), sw('results'), ew('.err'), ew('.pyc')))

        with futures.ThreadPoolExecutor(CPU_COUNT*128) as cleanup_pool:
            jobs = []
            for r in self.list_objects(self.jobtainer, select='name'):
                if should_cleanup(r):
                    jobs.append(
                        cleanup_pool.submit(self.client.delete_object,
                                            self.jobtainer, r))
                    time.sleep(1)
            if jobs:
                score = futures.wait(jobs, timeout=timeout)
                if score.done and not score.not_done:
                    print ("\nFinished cleaning up [%s object(s)] "
                           "from previous run." % len(score.done))
                else:
                    print ("Timed out after %ss: Cleaned up [%s object(s)] "
                           "and left behind some ( ~%s )"
                           % (timeout, len(score.done), len(score.not_done)))
            else:
                print "Nothing to clean up."

    def mapreduce_manifest(self, job_num, objects):

        objects = [o for o in objects if o]

        # tier is in job_num
        # use nullmapper after first tier
        if job_num.startswith('1'):
            mapper = MAPPER
        else:
            mapper = NULLMAPPER

        def _mapper_manifest(job_num, object_ref):
            if not object_ref:
                return
            if object_ref.endswith('*'):
                object_num = "glob-%s" % len(objects)
            else:
                object_num = objects.index(object_ref)
            if object_ref.startswith(SW):
                object_ref = object_ref[len(SW):]
            stderr_object_ref = os.path.split(object_ref)[-1]
            mapper_node = {"name": ("mapper-%s-%s"
                                    % (job_num, object_num)),
                           "exec": {"path": "file://python:python"}}
            mapper_connect = ["reducer"]
            mapper_devices = [
                {"name": "stdin",
                 "path": "%s" % mapper.format(jobtainer=self.jobtainer)},
                {"name": "input",
                 "path": "%s%s" % (SW, object_ref)},
                {"name": "stderr",
                 "path": ("%s" % ERRORS.format(jobtainer=self.jobtainer,
                                               object_ref=stderr_object_ref)),
                 "content_type": "text/plain"},
                {"name": "python"}
            ]
            mapper_node['devices'] = mapper_devices
            mapper_node['connect'] = mapper_connect
            return mapper_node

        cont_name = [p for p in
                     urlparse.urlparse(objects[0]).path.split('/') if p][0]

        # dont worry, its @cached
        cont_objects = {"%s/%s" % (cont_name, o)
                        for o in self.list_objects(cont_name, select='name')}

        diff = cont_objects - set(objects)
        if not any(diff):
            # we are simply looking at every object in an input container
            # use * globbing
            # this helps avoid the 64kb payload limit for > ~120 objects :)
            nodes = [_mapper_manifest(job_num, '%s/*' % cont_name)]
        else:
            nodes = [_mapper_manifest(job_num, n) for n in objects]

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


def make_json(thing):
    """Return a json-encoded string from a file, path, or dict."""
    if isinstance(thing, file):
        thing = thing.read()
    elif isinstance(thing, (list, dict)):
        thing = json.dumps(thing)
    elif os.path.exists(thing):
        with open(thing, 'r') as fthing:
            thing = fthing.read()
    return thing


@cached
def get_client(auth=None, user=None, key=None, **kwargs):
    """Return a client using v1 auth.

    Uses supplied keyword arguments, env vars, or a combination of both.
    """
    if not all((auth, user, key)):
        eauth, euser, ekey = map(os.getenv, ('ST_AUTH', 'ST_USER', 'ST_KEY'))
        auth, user, key = auth or eauth, user or euser, key or ekey

    if not all((auth, user, key)):
        raise AttributeError(
            "Swiftclient.Connection requires 'auth', 'user', 'key'.")
    # ZeroCloudConnection inherts from swiftclient.Connection
    # includes many kwarg/params... may be worth looking into
    client = swiftclient.Connection(auth, user, key, **kwargs)
    return setup_client(client)


def setup_client(client):
    """Set a bigger connection pool and other attributes.

    https://bugs.launchpad.net/python-swiftclient/+bug/1295812
    """
    client.url, client.token = client.get_auth()

    # prevent Connection pool is full, discarding connection:
    # zebra.zerovm.org !
    parsed_url, http_conn = client.http_connection()
    adapter = requests.adapters.HTTPAdapter(pool_maxsize=1000,
                                            pool_block=False)
    http_conn.request_session.mount(parsed_url.scheme + "://", adapter)
    client.http_conn = parsed_url, http_conn

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
        print "Max retries met. Fail."
        raise


def execute(*manifests, **clientkwargs):
    """Execute zerovm app given one or more manifest definitions.

    Return http (requests) response object(s).
    """
    partial = lambda job: _execute(job, **clientkwargs)
    with futures.ThreadPoolExecutor(CONCURRENT_JOBS) as tpool:
        results = []
        for result in tpool.map(partial, manifests, timeout=60*len(manifests)):
            # print "Finished a job: %s" % result
            results.append(result)
            time.sleep(1)
    return results


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
