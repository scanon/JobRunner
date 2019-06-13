from clients.CatalogClient import Catalog

class CatalogCache(object):
    def __init__(self, config, logger=None):
        self.catalog_url = config.get('catalog-service-url')
        self.catalog = Catalog(self.catalog_url, token=config['token'])
        self.catadmin = Catalog(self.catalog_url, token=config['admin_token'])
        self.module_cache = dict()
        self.logger = logger

    def get_volume_mounts(self, module, method, cgroup):
        if self.catadmin is None:
            return None
        req = {
            'module_name': module,
            'function_name': method,
            'client_group': cgroup
        }
        return self.catadmin.list_volume_mounts(req)

    def get_module_info(self, module, version):
        # Look up the module info
        if module not in self.module_cache:
            req = {'module_name': module}
            if version is not None:
                req['version'] = version
            module_info = self.catalog.get_module_version(req)
            self.module_cache[module] =  module_info
            git_url = module_info['git_url']
            git_commit = module_info['git_commit_hash']
            self.logger.log('Running module {}: url: {} commit: {}'.format(module, git_url, git_commit))
        else:
            module_info = self.module_cache[module]
            git_url = module_info['git_url']
            git_commit = module_info['git_commit_hash']
            version = module_info['version']
            f = 'WARNING: Module {} was already used once for this job. Using cached version: url: {} commit: {} version: {} release: release'
            self.logger.error(f.format(module, git_url, git_commit, version))
        return self.module_cache[module]
