import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

required_packages = ["boto3>=1.10.44, < 2.0"]

setuptools.setup(
    name="sagemaker_studio_sparkmagic_lib",
    version="0.1.4",
    author="Amazon Web Services",
    description="Python Command line tool to manage configuration of sparkmagic kernels on studio",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/aws-samples/sagemaker-studio-sparkmagic-conf",
    packages=["sagemaker_studio_sparkmagic_lib"],
    license="MIT-0 License",
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    python_requires=">=3.6",
    install_requires=required_packages,
    extras_require={"dev": ["black", "pytest"]},
    entry_points={
        "console_scripts": ["sm-sparkmagic=sagemaker_studio_sparkmagic_lib.cli:main"]
    },
    include_package_data=True,
    package_data={"sagemaker_studio_sparkmagic_lib": ["data/**"]},
)
