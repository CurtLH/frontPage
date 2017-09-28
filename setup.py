from setuptools import setup

requirements = [
    # package requirements go here
]

setup(
    name='frontPage',
    version='0.1.0',
    description="Short description",
    author="Curtis Hampton",
    author_email='CurtLHampton@gmail.com',
    url='https://github.com/CurtLH/frontPage',
    packages=['frontpage'],
    entry_points={
        'console_scripts': [
            'frontpage=frontpage.cli:cli'
        ]
    },
    install_requires=requirements,
    keywords='frontPage',
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
    ]
)
