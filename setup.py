from setuptools import setup

setup(
    name="zpm",
    version="0.16",
    author="Mike O'Malley",
    author_email="spuiousdata@gmail.com",
    license="MIT",
    packages=['zpm'],
    install_requires=[
        'prometheus-client',
    ],
    entry_points={
        'console_scripts': [
            'zpm = zpm.__main__:main',
        ],
    },
)
