import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="funnel-rocket",
    version="0.0.8",
    author="Elad Rosenheim, Avshalom Manevich",
    author_email="elad@dynamicyield.com",
    description="Cloud native distributed funnel queries",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/DynamicYieldProjects/funnel-rocket-oss",
    packages=setuptools.find_packages(),
    package_data={
        "frocket": ["resources/*.*"],
    },
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        # "License :: OSI Approved :: Apache Software License",  # TODO Pending approval!
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
    install_requires=['pyarrow>=2.0.0',
                      'pandas>=1.2.0',
                      'boto3>=1.16.0',
                      'redis>=3.5.0',
                      'tabulate>=0.8.0',
                      'prometheus_client>=0.9.0',
                      'flask>=1.1.0',
                      'jsonschema>=3.2.0',
                      'dataclasses-json>=0.5.2',
                      'inflection>=0.5.0',
                      'parsimonious>=0.8.0']
)
