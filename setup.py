from setuptools import setup, find_packages

setup(
    name='mist-cli',
    version='0.0.1',
    packages=find_packages(),
    py_modules=['main', 'app', 'models'],
    include_package_data=True,
    install_requires=[
        'Click', 'pyhocon', 'requests', 'texttable'
    ],
    entry_points='''
        [console_scripts]
        mist-cli=main:mist_cli
    ''',
)
