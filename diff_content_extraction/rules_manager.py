class RulesManager:
    def __init__(self, mongo_collection):
        self.db_collection = mongo_collection

    def dump_request_extractors(self, rules, request_id):
        extractors = dict(request_id=request_id,
                          extractors=rules)
        self.db_collection.insert_one(extractors)

    def load_request_extractors(self, request_id):
        # return self.db_collection.find_one({"domain": {"$regex": domain}})
        return self.db_collection.find_one({"request_id": request_id})

    @staticmethod
    def prepare_extractor(xpaths, suffix):
        return dict(
            suffix=suffix,  # TODO find common prefix for pages that can be used for extraction
            fields=xpaths
        )
