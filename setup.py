from setuptools import setup, find_packages

setup(
    name='mist-cli',
    version='0.0.1',
    packages=find_packages(),
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
        mist-cli=app.cli:mist_cli
    ''',
)
