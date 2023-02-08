# import logging
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
from azure.cosmos.partition_key import PartitionKey
import azure.functions as func
import simplejson as json


def find_container(db, id):
    print('1. Query for Container')

    containers = list(db.query_containers(
        {
            "query": "SELECT * FROM r WHERE r.id=@id",
            "parameters": [
                { "name":"@id", "value": id }
            ]
        }
    ))

    if len(containers) > 0:
        print('Container with id \'{0}\' was found'.format(id))
    else:
        print('No container with id \'{0}\' was found'. format(id))


def get_all_keywords(container):
    print('\n1.4 Querying for an  Item by Id\n')

    # enable_cross_partition_query should be set to True as the container is partitioned
    items = list(container.query_items(
        query="SELECT c.Keyword FROM c",
        enable_cross_partition_query=True
    ))
    return_array = []
    for item in items:
        return_array.append(item.get("Keyword"))

    print('Item queried by Id {0}'.format(items[0].get("Keyword")))
    return return_array

def read_Container(db, id):
    print("\n4. Get a Container by id")

    try:
        container = db.get_container_client(id)
        print('Container with id \'{0}\' was found, it\'s link is {1}'.format(container.id, container.container_link))
        return container

    except exceptions.CosmosResourceNotFoundError:
        print('A container with id \'{0}\' does not exist'.format(id))

def main(req: func.HttpRequest) -> func.HttpResponse:
    # name = req.params.get('name')
    # if not name:
    try:
        #https://github.com/Azure/azure-sdk-for-python/blob/master/sdk/cosmos/azure-cosmos/samples/database_management.py#L59-L67 
        
        HOST = 'https://hsadmin.documents.azure.com:443/' #config.settings['host']
        MASTER_KEY = 'RB6bqH8sAWsw07IIDFp5Nr5Hs5ScPlX0dsBXoB12dr969OTPMQSp5lBkTvBtzdDPHqvCtqE0uIdNBh2uQJy0VA==' #config.settings['master_key']
        DATABASE_ID = 'HCCosmosDB' #config.settings['database_id']
        CONTAINER_ID  = 'NewsContainer'
        
        client = cosmos_client.CosmosClient(HOST, {'masterKey': MASTER_KEY} )
        try:
            # setup database for this sample
            try:
                db = client.create_database(id=DATABASE_ID)
            except exceptions.CosmosResourceExistsError:
                db = client.get_database_client(DATABASE_ID)

            container = read_Container(db, CONTAINER_ID)
            key_dict = {}
            key_dict["Keyword"] = get_all_keywords(container)
            data = {"Keywords": key_dict}
            return func.HttpResponse(json.dumps(data, indent=4, sort_keys=True, default=str),status_code=200)  
        except exceptions.CosmosHttpResponseError as e:
            print('\nrun_sample has caught an error. {0}'.format(e.message))
        
    except ValueError:
        pass

    if name:
        data = {"name":name}
        return func.HttpResponse(json.dumps(data), status_code=200)
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
