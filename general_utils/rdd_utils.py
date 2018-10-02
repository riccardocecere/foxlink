def save_rdd(output,save,path_to_save):
    if (save):
        if path_to_save != None and path_to_save != '':
            output.saveAsTextFile(path_to_save)
    return None