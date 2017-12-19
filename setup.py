import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

about = {}
with open(os.path.join(here, 'mist', '__version__.py'), 'r') as f:
    exec(f.read(), about)

with open('README.rst', 'r') as f:
    readme = f.read()

setup(
    name=about['__title__'],
    version=about['__version__'],
    description=about['__description__'],
    long_description=readme,
    author=about['__author__'],
    author_email=about['__author_email__'],
    url=about['__url__'],
    license=about['__license__'],
    package_dir={'mist': 'mist'},
    include_package_data=True,
    zip_safe=False,
    classifiers=(
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ),

    packages=['mist'],
    install_requires=[
        'Click>=6', 'pyhocon', 'requests>=2.8.14', 'texttable'
    ],
    tests_require=[
        'pytest', 'requests-mock', 'mock>=2.0.0'
    ],
    test_suite='tests',
    entry_points='''
        [console_scripts]
        mist-cli=mist.cli:mist_cli
    ''',
)
