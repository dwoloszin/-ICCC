from getFileFromWeb import download_csv_files



def updateArchives():
  folder_path = "bulkload/" #web
  prefix = "BULKLOAD_CELLSECTOR_" #archivename
  save_path = "import/bulkload"  # Change this to your desired save path
  download_csv_files(folder_path,prefix,save_path)

  folder_path = "reports/" #web
  prefix = "MOBILESITE_" #archivename
  save_path = "import/Mobile"  # Change this to your desired save path
  download_csv_files(folder_path,prefix,save_path)

  folder_path = "reports/" #web
  prefix = "REPORT_SI_SI_" #archivename
  save_path = "import/SI"  # Change this to your desired save path
  download_csv_files(folder_path,prefix,save_path)

  folder_path = "reports/" #web
  prefix = "altaia_cell_" #archivename
  save_path = "import/ALTAIA/CELL"  # Change this to your desired save path
  download_csv_files(folder_path,prefix,save_path)

  folder_path = "nbulkload/" #web
  prefix = "dump_tpt_cellsector_" #archivename
  save_path = "import/nbulkload"  # Change this to your desired save path
  download_csv_files(folder_path,prefix,save_path)


