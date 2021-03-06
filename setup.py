from setuptools import setup

setup(
    name = "celerybeat-mongo",
    description = "A Celery Beat Scheduler that uses MongoDB to store both schedule definitions and status information",
    version = "1.0.1",
    license = "Apache License, Version 2.0",
    author = "Tarun Gupta",
    author_email = "tarun7nice@gmail.com",
    maintainer ="Tarun Gupta",
    maintainer_email = "tarun7nice@gmail.com",

    keywords = "python celery beat mongo",

    packages = [
        "celerybeatmongo"
    ],

   # install_requires=[
    #    'setuptools',
     #   'pymongo',
      #  'mongoengine',
       # 'celery',
    #]

)
