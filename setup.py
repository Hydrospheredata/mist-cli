from setuptools import setup, find_packages

setup(
    name='mist-cli',
    version='0.0.1',
    packages=['mist'],
    include_package_data=True,
    install_requires=[
        'Click', 'pyhocon', 'requests', 'texttable'
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
