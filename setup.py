try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

REQUIRES = [
    'python-swiftclient>=2.2.0',
    'futures>=2.1.6',
]

setup(name='mrzero',
      version='0.0.1',
      url='https://github.com/samstav/mrzero',
      install_requires=REQUIRES,
      py_modules=['mrzero'],
      scripts=['mrzero.py'],
     )
