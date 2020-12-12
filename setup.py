#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = ['Click>=7.0', ]

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest>=3', ]

setup(
    author="Nsikan Essien",
    author_email='audreyr@example.com',
    python_requires='>=3.5',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Experiment for creating annotation driven dataframe calcs",
    entry_points={
        'console_scripts': [
            'annotation_driven_dataframe_calcs=annotation_driven_dataframe_calcs.cli:main',
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='annotation_driven_dataframe_calcs',
    name='annotation_driven_dataframe_calcs',
    packages=find_packages(include=['annotation_driven_dataframe_calcs', 'annotation_driven_dataframe_calcs.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/noce2/annotation_driven_dataframe_calcs',
    version='0.1.0',
    zip_safe=False,
)
