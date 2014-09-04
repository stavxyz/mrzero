try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(name='mrzero',
      version='0.0.1',
      url='https://github.com/samstav/mrzero',
      py_modules=['mrzero'],
      scripts=['mrzero.py'],
     )
