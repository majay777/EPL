from setuptools import find_packages, setup

setup(
    name="epl",
    packages=find_packages(exclude=["epl_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
