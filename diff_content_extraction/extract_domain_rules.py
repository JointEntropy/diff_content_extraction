import json
from loguru import logger
from diff_content_extraction.base import extract_dataset
from diff_content_extraction.base import extract_pairwise_paths, extract_stable_xpaths_data
import os
import logging
from diff_content_extraction.rules_manager import RulesManager

logging.getLogger('__main__').setLevel('INFO')


def extract_domain_dataset(dataset_collection, domain, suffix):
    logger.debug('Fetch_dataset')
    items_iter = dataset_collection.find({"url": {"$regex": domain}}).limit(10)
    url_prefix = os.path.join(domain, suffix)
    items_iter = filter(lambda x: url_prefix in x['url'], items_iter)
    logger.debug('Extract dataset')
    dataset = extract_dataset(items_iter)
    return dataset


def extract_rules(dataset):
    logger.debug('Pairwise extract xpath')
    pairwise_xpaths_df = extract_pairwise_paths(dataset)
    logger.debug('Stabilize xpath')
    stable_xpaths = extract_stable_xpaths_data(pairwise_xpaths_df,
                                               min_support=0.6)
    return stable_xpaths


def extract_request_dataset(dataset_collection, request_id, limit=10):
    logger.debug('Fetch_dataset by request_id: {request_id}'.format(request_id=request_id))
    items_iter = dataset_collection.find({"request_id": request_id}).limit(limit)
    # url_prefix = os.path.join(domain, suffix)
    # TODO найти набор url`ов  с common longest string prefix, а остльные отбросить.
    # items_iter = filter(lambda x: url_prefix in x['url'], items_iter)
    # Update:
    # можно пойти по другому и сначала попытаться построить извлекатель на всём множестве и оставить только те страницы
    # у которых извлекатель работает стабильнее всего
    logger.debug('Extract dataset')
    dataset = extract_dataset(items_iter)
    return dataset


if __name__ == '__main__':
    from src.utils.mongo_conn import MongoConn
    CONFIG_PATH = os.environ['CONFIG_PATH']
    with open(CONFIG_PATH, 'r') as f:
        config = json.load(f)
    mongo_conn = MongoConn(config['mongo'])

    logger.info('Extract domain dataset.')
    # dataset = extract_domain_dataset(mongo_conn,
    #                                  domain='https://topliba.com',
    #                                  suffix='books/'
    # )
    request_id = '610ed710746aaf0882d500ea'
    dataset = extract_request_dataset(mongo_conn.get_collection(config['dataset_collection']),
                                      request_id=request_id)

    logger.info('Extract rules from dataset.')
    stable_xpaths = extract_rules(dataset)

    logger.info('Save extracted domain rules.')
    extractor_collection = mongo_conn.get_collection(config['extractor_collection'])
    rman = RulesManager(extractor_collection)
    extractors_lst = []
    for extracted_xpaths in [stable_xpaths]:
        extractors_lst.append(
            rman.prepare_extractor(extracted_xpaths, suffix='books')
        )
    rman.dump_request_extractors(extractors_lst, request_id=request_id)

    logger.info('Save extracted rules to collection')
    print(rman.load_request_extractors(request_id=request_id))



