import mlcroissant as mlc

ds = mlc.Dataset("output/croissant.json")
print(ds.metadata.record_sets[0].uuid)
record = next(iter(ds.records("mammograms")))
print(record.keys())
