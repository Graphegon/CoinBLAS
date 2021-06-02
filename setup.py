from setuptools import setup
import os

setup(
    name='coinblas',
    version='0.0.1',
    description='GraphBLAS based Bitcoin graph analysis.',
    author='Michel Pelletier',
    packages=['coinblas'],
    setup_requires=["pygraphblas"],
    install_requires=["pygraphblas"],
)

