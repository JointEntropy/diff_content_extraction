from functools import lru_cache
from tqdm import tqdm
from itertools import chain, product, combinations
from collections import Counter, defaultdict
import pandas as pd
from lxml import html, etree
import json
from loguru import logger
import os
from src.extraction.attribute_extraction import extract_attribs
from src.extraction.xpath_utils import  find_ancestor_node


def flat_xpaths_attrs(lst):
    for l in lst:
        for k in l:
            xpath = k['xpath']
            for attr in k['attribs']:
                yield (xpath, attr)


def collect_xpath_texts(xpath, node):
    if 'script' in xpath:
        return set()
    texts_set = set()
    for find_node in node.xpath(xpath):
        if getattr(find_node, 'text') and find_node.text.strip():
            texts_set.add(find_node.text)
    return texts_set


def jaccard(a, b):
    return len(a & b) / len(a | b)


def compare_xpath_entities(xpath, node1, node2):
    a_set = collect_xpath_texts(xpath, node1)
    b_set = collect_xpath_texts(xpath, node2)
    if len(a_set) == 0:
        return False
    if abs(jaccard(a_set, b_set)) < 0.05:
        return True
    return False


def extract_pairwise_paths(dataset):
    parse_html = lru_cache(html.fromstring)
    results_lst = []
    for a, b in tqdm(product(dataset.keys(), dataset.keys()), total=int(len(dataset) ** 2)):
        if a == b:
            continue
        r1_dom = parse_html(dataset[a]['html'])
        r2_dom = parse_html(dataset[b]['html'])
        tree = etree.ElementTree(r1_dom)
        collected_xpaths = []
        for e in r1_dom.iter():
            pth = tree.getpath(e)
            if compare_xpath_entities(pth, r1_dom, r2_dom):
                attribs = extract_attribs(pth, r1_dom, min_support=0.5)
                collected_xpaths.append(dict(xpath=pth, attribs=attribs))
        results_lst.append(dict(a=a, b=b, xpaths=collected_xpaths))
    res_df = pd.DataFrame(results_lst)
    return res_df


def extract_dataset(items_iter):
    """
    Формирует датасет из данныз соскрапленных страниц
    """
    pages_data = []
    for t in items_iter:
        pages_data.append(dict(url=t['_id'], html=t['html']))
    pages_df = pd.DataFrame(pages_data)
    #     pages_df = pages_df[pages_df['url'].apply(lambda x: 'https://topliba.com/books/' in x)]
    dataset = pages_df.set_index('url').to_dict('index')
    return dataset


def extract_stable_xpaths_data(pairwise_paths, min_support=0.):
    #     counts = Counter(list(chain.from_iterable(pairwise_paths['xpaths'])))
    counts = Counter(list(flat_xpaths_attrs(pairwise_paths['xpaths'])))
    stat_df = pd.DataFrame([dict(path=k, count=v) for k, v in counts.items()])
    stat_df['total'] = pairwise_paths.shape[0]
    stat_df['normalized'] = stat_df['count'] / stat_df['total']

    stat_df['root'] = stat_df['path'].apply(lambda x: find_ancestor_node(x[0]))
    stat_df['root_total'] = stat_df.groupby('root')['count'].transform(sum)
    stat_df['root_total_normalized'] = stat_df['root_total'] / stat_df['total']
    # TODO здесь ошибки со перекрытием подсчёта количества count_total может быть > total
    msk = (stat_df['root_total_normalized'] > min_support) | (stat_df['normalized'] > min_support)
    filtered_paths = stat_df.loc[msk, 'path'].tolist()

    result = defaultdict(list)
    for k in filtered_paths:
        result[k[0]].append(k[1])
    return result
