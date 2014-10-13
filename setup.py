try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

REQUIRES = [
    'turbolift>=2.1.3',
    'python-swiftclient>=2.2.0',
    'futures>=2.1.6',
    'sortedcontainers>=0.9.1',
    'wrapt>=1.9.0',
]

setup(
    name='mrzero',
    version='0.0.1',
    url='https://github.com/samstav/mrzero',
    install_requires=REQUIRES,
    py_modules=['mrzero'],
    scripts=['mrzero.py'],
)
