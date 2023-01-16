from setuptools import setup

tests_require = ["pytest"]

setup(
    name="sas-queue",
    version="0.1",
    description="A RabbitMQ based RPC + work queue",
    license="MIT",
    author="Eric Gjertsen",
    email="ericgj72@gmail.com",
    packages=[
        "sas_queue",
        "sas_queue.adapter",
        "sas_queue.adapter.mq",
        "sas_queue.model",
        "sas_queue.model.event",
        "sas_queue.util",
    ],
    # entry_points={"console_scripts": ["sas-queue = sas_queue.__main__:main"]},
    install_requires=["pika"],
    tests_require=tests_require,
    extras_require={"test": tests_require},  # to make pip happy
    zip_safe=False,  # to make mypy happy
)
