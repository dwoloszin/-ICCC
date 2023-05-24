import ImportDF




pathToImportBase = '\import\PBI_ICCC'
BASE_ANALISE = ImportDF.processArchive(pathToImportBase)

print(BASE_ANALISE)

pathToImportSI = '\import\SI'
SI = ImportDF.processArchive(pathToImportSI)


pathToImportMO = '\import\Mobile'
MO = ImportDF.processArchive(pathToImportMO)
