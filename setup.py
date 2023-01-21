from setuptools import setup

tests_require = ["pytest"]

setup(
    name="ttq",
    version="0.1",
    description="A RabbitMQ based RPC + work queue",
    license="MIT",
    author="Eric Gjertsen",
    email="ericgj72@gmail.com",
    packages=[
        "ttq",
        "ttq.command",
        "ttq.adapter",
        "ttq.model",
        "ttq.util",
        "ttq.util.concurrent",
    ],
    # entry_points={"console_scripts": ["ttq = ttq.__main__:main"]},
    install_requires=[
        "pika",
        "lmdbm @ git+ssh://git@github.com/Dobatymo/lmdb-python-dbm@master#egg=lmdbm",
    ],
    tests_require=tests_require,
    extras_require={"test": tests_require},  # to make pip happy
    zip_safe=False,  # to make mypy happy
)
