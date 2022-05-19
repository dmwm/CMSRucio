#!/usr/bin/python3

import typer
from strictyaml import load, YAMLValidationError
from lib.did_upload import datasetSchema, fileSchema, upload_dataset_and_create_rule, upload_file_and_create_rule, TemplateType
from rucio.client import Client

app = typer.Typer()

@app.command()
def upload_file(rse: str, temp_rse: str, lfn: str, file_path: str, copies: int, lifetime: int):
    """
    Upload a single file and create a rule for rse
    """

    typer.echo(f"File {file_path} will be uploaded to {rse}")

    upload_params= {
        "tempRSE": temp_rse,
    }

    rule_params = {
        "copies": copies,
        "rse": rse,
        "lifetime": lifetime
    }

    rucio_client = Client()
    upload_file_and_create_rule(rucio_client, file_path, lfn, upload_params, rule_params)
    


@app.command()
def upload_file_yaml(yamlfile: str):
    """
    Same as upload file
    Arguments specified in yaml files
    """
    with open(yamlfile) as f:
        yaml_string = f.read()
    
    try:
        params = load(yaml_string=yaml_string, schema=fileSchema)
    except YAMLValidationError as e:
        print(e)
        raise typer.Exit()
    
    params = params.data

    file_path = params["specs"]["filePath"]
    lfn = params["specs"]["lfn"]
    upload_params = params["specs"]["options"]["upload"]
    rule_params = params["specs"]["options"]["rule"]

    rucio_client = Client()
    upload_file_and_create_rule(rucio_client, file_path, lfn, upload_params, rule_params)


@app.command()
def upload_dataset(rse: str, temp_rse: str, dataset_path: str, dataset_name: str, copies: int, lifetime: int):
    """
    Upload a files specified in the folder and create a dataset containing those files, further create a rule for the dataset on specified rse
    """
    rucio_client = Client()

    upload_params= {
        "tempRSE": temp_rse,
    }

    rule_params = {
        "copies": copies,
        "rse": rse,
        "lifetime": lifetime
    }
    upload_dataset_and_create_rule(rucio_client, dataset_path, dataset_name, rule_params, upload_params)


@app.command()
def upload_dataset_yaml(yamlfile: str):
    """
    Same as upload dataset
    Arguments specified in yaml files
    """

    with open(yamlfile) as f:
        yaml_string = f.read()
    
    try:
        params = load(yaml_string=yaml_string, schema=datasetSchema)
    except YAMLValidationError as e:
        print(e)
        raise typer.Exit()
    
    params = params.data

    rucio_client = Client()
    dataset_path = params["specs"]["datasetPath"]
    dataset_name = params["specs"]["datasetName"]
    rule_params = params["specs"]["options"]["rule"]
    upload_params = params["specs"]["options"]["upload"]

    upload_dataset_and_create_rule(rucio_client, dataset_path, dataset_name, rule_params, upload_params)


@app.command()
def generate_yaml_template(type: TemplateType = typer.Argument(..., case_sensitive=False), dest: str = typer.Argument(".")):
    # TODO
    typer.echo(f"Under development")
    typer.echo(f"Please copy files from src/rucio-user-dm/templates/")
    
    

if __name__=="__main__":
    app()