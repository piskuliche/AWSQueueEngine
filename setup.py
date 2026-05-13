from setuptools import setup, find_packages

setup(
    name='awsqueueengine',
    version='0.1.0',
    description='Slurm-like SSH job manager for AWS GPU hosts',
    author='piskuliche',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        "boto3",
        "mailtrap",
        # tomllib is in the stdlib from 3.11 onward; tomli is the backport for 3.10.
        "tomli; python_version < '3.11'",
    ],
    entry_points={
        'console_scripts': [
            'awsqueueengine=awsqueueengine.cli:main',
            'awsqe-client=awsqueueengine.client.cli:main',
            'awsqe-host=awsqueueengine.host.cli:main',
        ],
    },
    python_requires='>=3.10',
)
