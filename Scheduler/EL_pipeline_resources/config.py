DB_SOURCE = {
    'dev': {
        'DATASET': {
            'USERNAME': 'root',
            'PASSWORD': '',
            'HOSTNAME': 'localhost'
        }
    }
}

DB_ME = {
    'dev': {
        'DATASET': {
            'USERNAME': 'root',
            'PASSWORD': '',
            'HOSTNAME': 'localhost',
            'DATASETNAME': 'my-data-set'
        }
    }
}

BQ_SC = {
    'data': {
        'location': "FileNeeded/dla-internship-program.json",
        'project_id': 'dla-internship-program'
    }
}

BQ_TG = {
    'data': {
        'location': "FileNeeded/dla-internship-program.json",
        'project_id': 'dla-internship-program'
    }
}

API_SC = {
    'data': {
        'url': 'https://jsonplaceholder.typicode.com/comments',
        'jsonLoc': 'C:/DLA/FileNeeded/jsonfile.json'
    }
}

FLAT_FILE = {
    'data': {
        'location_file': 'C:/DLA/FileNeeded/customer.csv'
    }
}

rw_path = {
    'data': {
        'path': 'dags/EL_pipeline_resources/RW_resource/hello_world.txt'
    }
}