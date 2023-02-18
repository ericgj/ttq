from setuptools import setup

install_requires = [
    "pika",
    "lmdbm @ git+ssh://git@github.com/Dobatymo/lmdb-python-dbm@master#egg=lmdbm",
]
try:
    import tomllib  # noqa
except ImportError:
    install_requires.append("tomli")

tests_require = ["pytest", "pytest-random-order"]

setup(
    name="ttq",
    version="0.3",
    description="A RabbitMQ based RPC + work queue",
    license="MIT",
    author="Eric Gjertsen",
    email="ericgj72@gmail.com",
    packages=[
        "ttq",
        "ttq.command",
        "ttq.adapter",
        "ttq.model",
        "ttq.model.event",
        "ttq.util",
        "ttq.util.concurrent",
    ],
    package_data={"ttq": ["py.typed"]},
    install_requires=install_requires,
    tests_require=tests_require,
    extras_require={"test": tests_require},  # to make pip happy
    zip_safe=False,  # to make mypy happy
)
