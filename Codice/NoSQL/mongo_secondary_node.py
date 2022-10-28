from werkzeug.wrappers import Request, Response
from werkzeug.serving import run_simple
from jsonrpc import JSONRPCResponseManager, dispatcher
import mongo_db_utils as mu

ShardAddress = None
ShardPort = None


def start_secondary_node(service_port, db_address, db_port, service_binding="0.0.0.0"):

    # memorizzazione delle impostazioni per l'accesso allo shard
    global ShardPort, ShardAddress
    ShardPort = db_port
    ShardAddress = db_address

    # avvio del servizio che rimane in ascolto per chiamate future dal nodo primario
    run_simple(service_binding, service_port, secondary_web_service)


@Request.application
def secondary_web_service(request):

    # definizione dei metodi del servizio web
    dispatcher["preprocess_tweets"] = preprocess_tweets
    dispatcher["is_preprocess_complete"] = is_preprocess_complete

    response = JSONRPCResponseManager.handle(request.data, dispatcher)
    return Response(response.json, mimetype='application/json')


def is_preprocess_complete():
    # il preprocessing si intende terminato quando tutti i thread avranno finito l'esecuzione
    if mu.running_threads_preprocessing_tweets == 0:
        return "ok"
    else:
        return "wait"


def preprocess_tweets():

    print("Start to preprocess all shard tweets")
    mu.preprocess_all_shard_tweets(ShardAddress, ShardPort, wait_for_threads=False)
    print("Preprocessing started")

    return "wait"