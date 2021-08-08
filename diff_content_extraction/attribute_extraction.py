from itertools import chain, product, combinations
from collections import Counter, defaultdict
import numpy as np


def gini(x):
    x = np.array(x, dtype=np.float32)
    n = len(x)
    diffs = sum(abs(i - j) for i, j in combinations(x, r=2))
    return diffs / (2 * n**2 * x.mean())


def extract_attribs(xpath, node, min_support=0.2):
    """
    Добавить порог по gini.
    Нужно с каждого xpath результата собирать поля со всех атрибутов.
    Там где атрибуты меняются сильно, значит имеет смысл хранить.
    """
    attribs_count = Counter()
    total_cnt = 0
    for e in node.xpath(xpath):
        attribs_count.update(e.attrib)
        total_cnt += 1
    return {k for k, cnt in attribs_count.items() if cnt/total_cnt>min_support}
