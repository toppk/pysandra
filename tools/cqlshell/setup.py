import typing
from pathlib import Path

from setuptools import setup


def get_packages(package: str) -> typing.List[str]:
    return [str(path.parent) for path in Path(package).glob("**/__init__.py")]


setup(
    name="cqlshell",
    version="0.2",
    packages=get_packages("cqlshell"),
    install_requires=["termtables"],
    entry_points={
        "console_scripts": [
            "cqlshell=cqlshell.main:cli",
            "testasync=cqlshell.testasync:main",
        ]
    },
)
