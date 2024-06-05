package handler

import "errors"

var ErrCheckUpdateImmutableFields = errors.New("update immutable fields error")
var ErrCheckOutputOnlyFields = errors.New("can not contain output only fields")
var ErrCheckRequiredFields = errors.New("required fields missing")
var ErrFieldMask = errors.New("field mask error")
var ErrResourceID = errors.New("resource ID error")
var ErrSematicVersion = errors.New("not a legal version, should be the format vX.Y.Z or vX.Y.Z-identifiers")
var ErrUpdateMask = errors.New("update mask error")
var ErrConnectorNamespace = errors.New("can not use other's connector")

var ErrCreateKnowledgeBase = errors.New("failed to create knowledge base")
var ErrGetKnowledgeBases = errors.New("failed to get knowledge bases")
var ErrUpdateKnowledgeBase = errors.New("failed to update knowledge base")
var ErrDeleteKnowledgeBase = errors.New("failed to delete knowledge base")
var ErrInvalidKnowledgeBaseName = errors.New("invalid knowledge base Name")