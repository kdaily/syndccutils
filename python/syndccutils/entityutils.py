import synapseclient
import sys
import multiprocessing
import itertools
import multiprocessing.dummy

def deleteWithRetry(obj, version):
    sys.stderr.write("Deleting %s.%s\n" % (obj, version))

    try:
        return synapseclient.retry._with_retry(syn.delete(obj=obj, version=version))
    except TypeError:
        return None

   
def deleteEntityVersions(syn, entity, versions=None, dryRun=False):
    """Delete selected versions of an entity.

    If 'versions' parameter is not specified, delete all except first version.

    """

    e = synapseclient.utils.id_of(entity)

    entityVersions = list(syn._GET_paginated("/entity/%s/version" % e))

    if versions:
        entityVersions = [x for x in entityVersions if x['versionNumber'] in versions]
    else:
        x = entityVersions.pop()

    if not entityVersions:
        sys.stderr.write("Nothing to delete, only one version: %s.%s\n" % (x['id'], x['versionNumber']))
        return None

    if dryRun:
        map(lambda x: sys.stderr.write("Deleting %s.%s\n" % (x['id'], x['versionNumber'])), entityVersions)
    else:
        map(lambda x: deleteWithRetry(obj=x['id'], version=x['versionNumber']), entityVersions)

# def deleteVersionsFromFileView(syn, fileViewId):
#     d = syn.tableQuery("SELECT id FROM %s where currentVersion > 1" % (fileViewId, )).asDataFrame()
#     pool = multiprocessing.dummy.Pool(8)
#     pool.map(lambda x: deleteEntityVersions(syn, x, dryRun=False), d.id.tolist())
