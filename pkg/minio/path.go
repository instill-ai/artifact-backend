package minio

func GetUploadedFilePathInKnowledgeBase(kbUID, fileUID, fileExt string) string {
	return kbUID + "/uploaded-file/" + fileUID + "." + fileExt
}

func GetConvertedFilePathInKnowledgeBase(kbUID, ConvertedFileUID, fileExt string) string {
	return kbUID + "/converted-file/" + ConvertedFileUID + "." + fileExt
}

func GetChunkPathInKnowledgeBase(kbUID, chunkUID string) string {
	return kbUID + "/chunk/" + chunkUID + ".txt"
}
