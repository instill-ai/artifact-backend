# Changelog

## [0.34.0](https://github.com/instill-ai/artifact-backend/compare/v0.33.0...v0.34.0) (2025-10-31)


### Features

* **ai:** add multi-provider support with OpenAI for legacy embeddings ([#271](https://github.com/instill-ai/artifact-backend/issues/271)) ([6896341](https://github.com/instill-ai/artifact-backend/commit/689634194af655d533a3f6dda4afd98b110abfb3))
* **file,retrieval:** add tag filter to similarity chunk search ([#266](https://github.com/instill-ai/artifact-backend/issues/266)) ([d384282](https://github.com/instill-ai/artifact-backend/commit/d3842822277abb5574379e5670e3d97e017a97ed))
* **file:** add tag update endpoint ([#267](https://github.com/instill-ai/artifact-backend/issues/267)) ([fde1952](https://github.com/instill-ai/artifact-backend/commit/fde1952577d53e944c65e8122ca79e80f555c2f8))
* **kb:** add abort capability and enum standardization for zero-downtime update ([#274](https://github.com/instill-ai/artifact-backend/issues/274)) ([89d3bf7](https://github.com/instill-ai/artifact-backend/commit/89d3bf7821ff876624a53a6b17d378a482fb7f78))
* **kb:** implement zero-downtime KB update ([#273](https://github.com/instill-ai/artifact-backend/issues/273)) ([28ca0d0](https://github.com/instill-ai/artifact-backend/commit/28ca0d0a81d595c040a50ba002da10c00eca1771))
* **rag:** implement zero-downtime KB update framework with system config management ([#275](https://github.com/instill-ai/artifact-backend/issues/275)) ([6b3733d](https://github.com/instill-ai/artifact-backend/commit/6b3733d1ca1be4840c33a4eb2cdfad28ea161e4d))
* **worker:** implement Temporal worker to replace Go routine ([#264](https://github.com/instill-ai/artifact-backend/issues/264)) ([941299a](https://github.com/instill-ai/artifact-backend/commit/941299aed69734460c16071c9882fb2c9827f1c6))


### Miscellaneous

* **file:** add post-file-processing hook ([#281](https://github.com/instill-ai/artifact-backend/issues/281)) ([850e6a1](https://github.com/instill-ai/artifact-backend/commit/850e6a1a7404cab4538a0e36278edd55cefe6512))
* **main:** fix manifest.json release version ([6ac591d](https://github.com/instill-ai/artifact-backend/commit/6ac591de088523e5c37363d53ae675b6c7978be1))
* **main:** revert back to previous value scheme ([#268](https://github.com/instill-ai/artifact-backend/issues/268)) ([0511ed1](https://github.com/instill-ai/artifact-backend/commit/0511ed14046ba6fce3106da6af2db85cc31c90fe))
* release v0.34.0 ([851a3dc](https://github.com/instill-ai/artifact-backend/commit/851a3dc21736d9fc598cfcc219b156215b09a170))


### Refactor

* **ai:** move AI client from internal to public API ([#280](https://github.com/instill-ai/artifact-backend/issues/280)) ([d6dd9b4](https://github.com/instill-ai/artifact-backend/commit/d6dd9b4f8d6c92290fd87c608337d634d2becd27))
* **api:** align artifact backend with Google AIP standards ([#278](https://github.com/instill-ai/artifact-backend/issues/278)) ([128a286](https://github.com/instill-ai/artifact-backend/commit/128a286c64ba141eb81e0f252838d5839f6f1de2))
* **api:** rename Catalog to Knowledge Base across entire codebase ([#279](https://github.com/instill-ai/artifact-backend/issues/279)) ([edade9d](https://github.com/instill-ai/artifact-backend/commit/edade9d798016954c2773964fd0f768469e2b0f0))
* **artifact:** remove deprecated start_pos and end_pos fields from Chunk message ([#282](https://github.com/instill-ai/artifact-backend/issues/282)) ([eeb433d](https://github.com/instill-ai/artifact-backend/commit/eeb433df49cb42a6b0261302033a5b7069d0dbd1))
* **artifact:** simplify chunk retrieval and align chunk type naming with protobuf ([#270](https://github.com/instill-ai/artifact-backend/issues/270)) ([04a3c69](https://github.com/instill-ai/artifact-backend/commit/04a3c693d9714820738b5294f5e938095781f711))
* **embeddings:** replace preset pipeline with direct model API call ([#269](https://github.com/instill-ai/artifact-backend/issues/269)) ([89d528f](https://github.com/instill-ai/artifact-backend/commit/89d528fb2107b9140e4d4fb9b8af59865925a4fc))
* **worker:** implement symmetric cleanup patterns and simplify embedding workflow ([#272](https://github.com/instill-ai/artifact-backend/issues/272)) ([e29b676](https://github.com/instill-ai/artifact-backend/commit/e29b676b03234f92c03a5d3fd161547b810d38f5))

## [0.32.1](https://github.com/instill-ai/artifact-backend/compare/v0.32.0...v0.32.1) (2025-09-30)


### Features

* **chunking:** page by chunk when page delimiters are present ([#262](https://github.com/instill-ai/artifact-backend/issues/262)) ([13b766e](https://github.com/instill-ai/artifact-backend/commit/13b766ecdbe933dad419a9a5fd978c631589fbcd))
* **conversion:** add char length to text files ([#261](https://github.com/instill-ai/artifact-backend/issues/261)) ([c92684f](https://github.com/instill-ai/artifact-backend/commit/c92684f4425cdf5d108552b2a2dbd6a9e3e2dc02))
* **conversion:** extract pages in conversion step ([#260](https://github.com/instill-ai/artifact-backend/issues/260)) ([925dd1c](https://github.com/instill-ai/artifact-backend/commit/925dd1ce9fc3c040b4f98617f861016cfeb2c643))


### Bug Fixes

* **reprocess:** clean up data from previous file processing ([#259](https://github.com/instill-ai/artifact-backend/issues/259)) ([56f0368](https://github.com/instill-ai/artifact-backend/commit/56f036855cf6e13cff35b651de954e2e8bb6d5b8))
* **usage:** add missing error filtering for users/admin ([#257](https://github.com/instill-ai/artifact-backend/issues/257)) ([c42ac14](https://github.com/instill-ai/artifact-backend/commit/c42ac14f87a1594406307d5511e5535f6bd07d62))


### Miscellaneous

* **blob:** make file blob paths more descriptive ([#263](https://github.com/instill-ai/artifact-backend/issues/263)) ([c23ad31](https://github.com/instill-ai/artifact-backend/commit/c23ad315f6e9b3e8decf6810edf67dab8fe9bd30))

## [0.32.0](https://github.com/instill-ai/artifact-backend/compare/v0.31.1...v0.32.0) (2025-09-18)


### Features

* **chunking:** store page references with chunk records ([#255](https://github.com/instill-ai/artifact-backend/issues/255)) ([94d1e2e](https://github.com/instill-ai/artifact-backend/commit/94d1e2e10dae2e6f3a8fd97c86c06bae7353965f))
* **kbfile:** return length in conversion step and store it as metadata ([#251](https://github.com/instill-ai/artifact-backend/issues/251)) ([4178d05](https://github.com/instill-ai/artifact-backend/commit/4178d05cdd966e97f95924626fb48ed83c5c4863))


### Bug Fixes

* **integration-test:** fix file summary check  ([#254](https://github.com/instill-ai/artifact-backend/issues/254)) ([6cccfc1](https://github.com/instill-ai/artifact-backend/commit/6cccfc1ccde4403ce91caf2cf8b11a3e4ea8f420))
* **preset:** revert wrongful update in conversion pipeline ID ([#252](https://github.com/instill-ai/artifact-backend/issues/252)) ([0737b0c](https://github.com/instill-ai/artifact-backend/commit/0737b0c3363f697801ebb7f8e1ce98cd6b430440))


### Miscellaneous

* release v0.32.0 ([#256](https://github.com/instill-ai/artifact-backend/issues/256)) ([2e90cd4](https://github.com/instill-ai/artifact-backend/commit/2e90cd4677be65f8a6890455337dfe8eff2b9218))

## [0.31.1](https://github.com/instill-ai/artifact-backend/compare/v0.31.0...v0.31.1) (2025-09-12)


### Features

* **chunk:** return page-level citation context in file list and similar chunk search ([#248](https://github.com/instill-ai/artifact-backend/issues/248)) ([5be7715](https://github.com/instill-ai/artifact-backend/commit/5be77156b89760829e2256ff419c7a2f57ca4736))


### Bug Fixes

* **conversion:** do not set default conversion pipeline in catalog record ([#250](https://github.com/instill-ai/artifact-backend/issues/250)) ([c629112](https://github.com/instill-ai/artifact-backend/commit/c6291123f07ad4a5c4cc33762b0985a1db99c775))

## [0.31.0](https://github.com/instill-ai/artifact-backend/compare/v0.30.1...v0.31.0) (2025-09-09)


### Features

* **kbfile:** add status filter to catalog file list ([#239](https://github.com/instill-ai/artifact-backend/issues/239)) ([#244](https://github.com/instill-ai/artifact-backend/issues/244)) ([1836e09](https://github.com/instill-ai/artifact-backend/commit/1836e0967fce89307e55b19c8497b9e90b5e7057))


### Bug Fixes

* **kbfile:** reprocessing fixes ([#246](https://github.com/instill-ai/artifact-backend/issues/246)) ([c4ec3a0](https://github.com/instill-ai/artifact-backend/commit/c4ec3a0e6edd9f137c6272f9b4dadb83e8cb51a2))


### Miscellaneous

* **x:** update x to v0.10.0-alpha ([#247](https://github.com/instill-ai/artifact-backend/issues/247)) ([0ebd3cd](https://github.com/instill-ai/artifact-backend/commit/0ebd3cd4da154039ac333c079f85bc2dc1bab2fc))


### Tests

* **integration:** init integration tests ([#242](https://github.com/instill-ai/artifact-backend/issues/242)) ([84d54b5](https://github.com/instill-ai/artifact-backend/commit/84d54b5b59dea829147d53a98a41fa1fedc62240))

## [0.30.1](https://github.com/instill-ai/artifact-backend/compare/v0.30.0...v0.30.1) (2025-09-02)


### Features

* **conversion:** remove vlm_model variable from conversion pipeline ([#240](https://github.com/instill-ai/artifact-backend/issues/240)) ([1a2738a](https://github.com/instill-ai/artifact-backend/commit/1a2738a1abc9fa3e188f0f7479393e215ff320cb))
* **kbfile:** add status filter to catalog file list ([#239](https://github.com/instill-ai/artifact-backend/issues/239)) ([d69c2df](https://github.com/instill-ai/artifact-backend/commit/d69c2df3656e68d0b8dd7a25ad75ea6647a1597b))


### Bug Fixes

* **catalog:** fix uploaded filename suffix uppercase issue ([#238](https://github.com/instill-ai/artifact-backend/issues/238)) ([72f8c71](https://github.com/instill-ai/artifact-backend/commit/72f8c719b15b88c83d553250ac27fb6ef97253b0))


### Miscellaneous

* release v0.30.1 ([#243](https://github.com/instill-ai/artifact-backend/issues/243)) ([185791a](https://github.com/instill-ai/artifact-backend/commit/185791a6d9945de5ea4812c4ac66872a711b7459))

## [0.30.0](https://github.com/instill-ai/artifact-backend/compare/v0.29.0...v0.30.0) (2025-08-26)


### Features

* **embedding:** filter embedding search with file UID list ([#232](https://github.com/instill-ai/artifact-backend/issues/232)) ([eb4585a](https://github.com/instill-ai/artifact-backend/commit/eb4585ad0766dbe70d58da23422cdcf126c4e559))
* **file:** allow per-file conversion pipeline setting ([#234](https://github.com/instill-ai/artifact-backend/issues/234)) ([3b02e30](https://github.com/instill-ai/artifact-backend/commit/3b02e306d7b1645c513123b1d8a0184db4f793f9))


### Bug Fixes

* **kbfile:** use filename in object when uploading a new file by reference ([#236](https://github.com/instill-ai/artifact-backend/issues/236)) ([7133d10](https://github.com/instill-ai/artifact-backend/commit/7133d10abba9a88f2e9c4e5afbb9ba35270abd05))


### Miscellaneous

* **minio:** use new bucket names ([#235](https://github.com/instill-ai/artifact-backend/issues/235)) ([3dd4d9f](https://github.com/instill-ai/artifact-backend/commit/3dd4d9f6f3e9111b729435a5921466e45b72ab61))
* release v0.30.0 ([#237](https://github.com/instill-ai/artifact-backend/issues/237)) ([dc211c6](https://github.com/instill-ai/artifact-backend/commit/dc211c681359a962d14e562c8179bfb11c8ecb3c))

## [0.29.0](https://github.com/instill-ai/artifact-backend/compare/v0.28.0...v0.29.0) (2025-07-31)


### Bug Fixes

* **repository:** fix wrong page token handling ([#230](https://github.com/instill-ai/artifact-backend/issues/230)) ([2eec4ad](https://github.com/instill-ai/artifact-backend/commit/2eec4ade34058cb9cf041a6fd7df94596b87f85f))
* **service:** the user without tier will fallback to TierFree ([#229](https://github.com/instill-ai/artifact-backend/issues/229)) ([54ca6df](https://github.com/instill-ai/artifact-backend/commit/54ca6df12d0366c2acaefd760076d0bebe1aa97d))


### Miscellaneous

* **mgmt:** use new subscription states ([#225](https://github.com/instill-ai/artifact-backend/issues/225)) ([d3b60cf](https://github.com/instill-ai/artifact-backend/commit/d3b60cfd5609a6766db7a5c696f938fb2b62914f))
* release v0.29.0 ([8ecb604](https://github.com/instill-ai/artifact-backend/commit/8ecb604f0cae17aac90475ce47860d30ccb17794))

## [0.28.0](https://github.com/instill-ai/artifact-backend/compare/v0.27.0...v0.28.0) (2025-07-16)


### Features

* **file:** allow duplicate filenames ([#218](https://github.com/instill-ai/artifact-backend/issues/218)) ([a1b3049](https://github.com/instill-ai/artifact-backend/commit/a1b304956149799eb6eee713584b214b1b96b260))
* **object:** directly use MinIO pre-signed URL for uploading and downloading objects ([#212](https://github.com/instill-ai/artifact-backend/issues/212)) ([cb81c9b](https://github.com/instill-ai/artifact-backend/commit/cb81c9b184b6356b917d0f42c806b558c57c9590))
* **otel:** integrate OTEL using gRPC interceptor ([#223](https://github.com/instill-ai/artifact-backend/issues/223)) ([57e8bb7](https://github.com/instill-ai/artifact-backend/commit/57e8bb7d2c587c79a7351417f5b436ecb96ec581))


### Bug Fixes

* **handler:** fix wrong IsUploaded check ([#217](https://github.com/instill-ai/artifact-backend/issues/217)) ([0f71810](https://github.com/instill-ai/artifact-backend/commit/0f71810e41af290ad20b5b1e10e725ecd8b0254a))
* **milvus:** retrieve file_uid metadata only when present in schema ([#220](https://github.com/instill-ai/artifact-backend/issues/220)) ([aae7d94](https://github.com/instill-ai/artifact-backend/commit/aae7d941de3a13e64ca1f4e8b14fe3fa793d20db))
* **service:** fix the wrong object size ([#222](https://github.com/instill-ai/artifact-backend/issues/222)) ([fa26ce0](https://github.com/instill-ai/artifact-backend/commit/fa26ce08350704c9d1f58f7c7f3754aea3505325))
* **service:** use base64.URLEncoding to encode the blob URL ([#221](https://github.com/instill-ai/artifact-backend/issues/221)) ([25b8b41](https://github.com/instill-ai/artifact-backend/commit/25b8b417e8c9dbf7a0ea8c0f4d74f9b2dc904d21))


### Miscellaneous

* **codehealth:** refactor pipeline init and service ([#216](https://github.com/instill-ai/artifact-backend/issues/216)) ([765d5cc](https://github.com/instill-ai/artifact-backend/commit/765d5cc29b458cbb98a575d0d6b8385393abcd64))
* **dep:** bump up usage-client version ([#219](https://github.com/instill-ai/artifact-backend/issues/219)) ([b1b123d](https://github.com/instill-ai/artifact-backend/commit/b1b123d70d888054b93524cc08e52221a3331c8d))
* release v0.28.0 ([5181620](https://github.com/instill-ai/artifact-backend/commit/5181620ae2b8435e21236180652762b20c8e31ea))

## [0.27.0](https://github.com/instill-ai/artifact-backend/compare/v0.26.0...v0.27.0) (2025-07-01)


### Features

* **catalog:** specify document conversion pipelines in catalog update ([#214](https://github.com/instill-ai/artifact-backend/issues/214)) ([8737661](https://github.com/instill-ai/artifact-backend/commit/87376615c9ce9edb14eee3e7367a5badb09462e0))
* **conversion:** parse documents with automatic classification on Agent requests ([#201](https://github.com/instill-ai/artifact-backend/issues/201)) ([7b96f1b](https://github.com/instill-ai/artifact-backend/commit/7b96f1b620e40167a58304c07a67bf047e527bd8))


### Bug Fixes

* **handler:** fix the empty download link for files uploaded in legacy way ([#205](https://github.com/instill-ai/artifact-backend/issues/205)) ([4f64edc](https://github.com/instill-ai/artifact-backend/commit/4f64edc0f4d650a161baede6c246574b8b3a48fc))
* **preset:** revert use of docling converter in document component ([#206](https://github.com/instill-ai/artifact-backend/issues/206)) ([9e43ca1](https://github.com/instill-ai/artifact-backend/commit/9e43ca1d083655d9002ef3540d07cf819b0c2823))
* **preset:** revert use of docling converter in document component ([#207](https://github.com/instill-ai/artifact-backend/issues/207)) ([b567575](https://github.com/instill-ai/artifact-backend/commit/b567575b16782d59c327d21cc85821717f0cd99d))
* **preset:** use docling converter in document component ([#204](https://github.com/instill-ai/artifact-backend/issues/204)) ([0fe3e6a](https://github.com/instill-ai/artifact-backend/commit/0fe3e6ad4f32e27f65af2e533b0465d4bd2b3ecb))


### Miscellaneous

* **conversion:** fallback to heuristic method on parsing-router pipeline ([#208](https://github.com/instill-ai/artifact-backend/issues/208)) ([6222063](https://github.com/instill-ai/artifact-backend/commit/62220637a41e2c5e9633f6097d8787c1aeadbbad))
* **deadcode:** remove deadcode and extract gRPC clients ([#211](https://github.com/instill-ai/artifact-backend/issues/211)) ([bb89ce6](https://github.com/instill-ai/artifact-backend/commit/bb89ce61e4734b7604f17b6e1fed02769a1ca61f))
* **kbfile:** sort catalog files by descending creation time ([#209](https://github.com/instill-ai/artifact-backend/issues/209)) ([c7684a1](https://github.com/instill-ai/artifact-backend/commit/c7684a19fedbe239bb1a624c925b807fbf861f9a))
* **main:** release v0.27.0 ([#213](https://github.com/instill-ai/artifact-backend/issues/213)) ([5a9bf1e](https://github.com/instill-ai/artifact-backend/commit/5a9bf1e11f17ff38468e8e0ce5e4f0fb598cded5))

## [0.26.0](https://github.com/instill-ai/artifact-backend/compare/v0.25.2-rc...v0.26.0) (2025-06-18)


### Features

* **catalog:** specify document conversion pipelines in catalog creation ([#197](https://github.com/instill-ai/artifact-backend/issues/197)) ([08e124e](https://github.com/instill-ai/artifact-backend/commit/08e124ef5e591298e6f4729369a115f33e94ab6b))


### Bug Fixes

* **subscription:** update enum for starter plan ([#194](https://github.com/instill-ai/artifact-backend/issues/194)) ([922e2ff](https://github.com/instill-ai/artifact-backend/commit/922e2ffd8525639c68ddb73d8287242d7a7f6aba))


### Miscellaneous

* release v0.26.0 ([37124ba](https://github.com/instill-ai/artifact-backend/commit/37124ba499b6b118bd1bb3799ec54ad3cc3a7ac7))

## [0.25.2-rc](https://github.com/instill-ai/artifact-backend/compare/v0.25.1-alpha...v0.25.2-rc) (2025-06-11)


### Miscellaneous

* **kbfile:** do not convert files using Docling model ([#190](https://github.com/instill-ai/artifact-backend/issues/190)) ([0ca41fc](https://github.com/instill-ai/artifact-backend/commit/0ca41fc72afb2b5b98b96b975d199550d8f41612))
* **main:** release v0.25.2-rc ([#193](https://github.com/instill-ai/artifact-backend/issues/193)) ([8a9bd86](https://github.com/instill-ai/artifact-backend/commit/8a9bd86196e206b921d5681a48d6dcc24a92b848))
* **subscription:** keep Pro plan until next release ([#192](https://github.com/instill-ai/artifact-backend/issues/192)) ([2ee3a07](https://github.com/instill-ai/artifact-backend/commit/2ee3a07c8b949fd73e7f8db631a91068ed8e0e6a))

## [0.25.1-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.25.0-alpha...v0.25.1-alpha) (2025-06-10)


### Bug Fixes

* **subscription:** update enum for starter plan ([#188](https://github.com/instill-ai/artifact-backend/issues/188)) ([304325c](https://github.com/instill-ai/artifact-backend/commit/304325c13ba46d29f55feea64d6127af1c654c9c))


### Miscellaneous

* **kbfile:** do not convert files using Docling model ([#186](https://github.com/instill-ai/artifact-backend/issues/186)) ([b411242](https://github.com/instill-ai/artifact-backend/commit/b411242e89f1963d7c66f26c302ad3d4af24d0f5))
* **kbfile:** use Docling for file conversion' ([#189](https://github.com/instill-ai/artifact-backend/issues/189)) ([a81e31d](https://github.com/instill-ai/artifact-backend/commit/a81e31dd548f6d45a202122f773e4d35bcb1694c))

## [0.25.0-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.24.5-alpha...v0.25.0-alpha) (2025-06-03)


### Features

* **kbfile:** use ExternalMetadata to hold request context ([#183](https://github.com/instill-ai/artifact-backend/issues/183)) ([f0e9f5c](https://github.com/instill-ai/artifact-backend/commit/f0e9f5cb47432bfea274b535a7ebf9109980d285))


### Miscellaneous

* **domain:** update production domain ([#184](https://github.com/instill-ai/artifact-backend/issues/184)) ([e83f8d6](https://github.com/instill-ai/artifact-backend/commit/e83f8d6fe3f548987bb216722fbf6056993e34ba))
* rollback parsing logic ([#185](https://github.com/instill-ai/artifact-backend/issues/185)) ([5a9af09](https://github.com/instill-ai/artifact-backend/commit/5a9af0987e0d41897b9ffd59c07ef83f44fe45e6))

## [0.24.5-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.24.4-alpha...v0.24.5-alpha) (2025-04-24)


### Bug Fixes

* **artifact:** use VLM pipeline to convert ppt/pptx files ([#178](https://github.com/instill-ai/artifact-backend/issues/178)) ([1de8aa1](https://github.com/instill-ai/artifact-backend/commit/1de8aa1a7368aece9492a891ba9801642e129ad0))

## [0.24.4-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.24.3-alpha...v0.24.4-alpha) (2025-04-23)


### Bug Fixes

* resolve the issue where non-document files cannot be processed ([#176](https://github.com/instill-ai/artifact-backend/issues/176)) ([9db1b57](https://github.com/instill-ai/artifact-backend/commit/9db1b576aaa9f4db05957dc6ca77c18b35809514))

## [0.24.3-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.24.2-alpha...v0.24.3-alpha) (2025-04-16)


### Bug Fixes

* **chunk:** prevent the full table query ([#173](https://github.com/instill-ai/artifact-backend/issues/173)) ([e33ed18](https://github.com/instill-ai/artifact-backend/commit/e33ed1816b8cd61865117b9d67bbd97cee729a9a))

## [0.24.2-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.24.1-alpha...v0.24.2-alpha) (2025-03-31)


### Bug Fixes

* **handler:** fix embedded downloadURL bug ([#171](https://github.com/instill-ai/artifact-backend/issues/171)) ([0c9a908](https://github.com/instill-ai/artifact-backend/commit/0c9a9084f67590201bc8554a98f27f6b1f3c228c))

## [0.24.1-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.24.0-alpha...v0.24.1-alpha) (2025-03-30)


### Miscellaneous Chores

* release v0.24.1-alpha ([569fc59](https://github.com/instill-ai/artifact-backend/commit/569fc59a6cc3c92a6e1de33568d38ea9556aa5d7))

## [0.24.0-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.23.1-alpha...v0.24.0-alpha) (2025-03-28)


### Features

* **artifact:** implement get summary endpoint ([#162](https://github.com/instill-ai/artifact-backend/issues/162)) ([decfaf7](https://github.com/instill-ai/artifact-backend/commit/decfaf78d914ea4ebd4d682411f2d83c497c9e87))
* **handler:** add get chat file ([#168](https://github.com/instill-ai/artifact-backend/issues/168)) ([704c4b8](https://github.com/instill-ai/artifact-backend/commit/704c4b8a31b96c3151c66acfc289004ddc05ba15))

## [0.23.1-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.23.0-alpha...v0.23.1-alpha) (2025-02-26)


### Bug Fixes

* **search:** support metadata-less embeddings search ([#159](https://github.com/instill-ai/artifact-backend/issues/159)) ([c124dcc](https://github.com/instill-ai/artifact-backend/commit/c124dcc4953cd14bfe7f64726f3ad56caa0e6606))

## [0.23.0-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.22.0-alpha...v0.23.0-alpha) (2025-02-25)


### Features

* **artifact:** add move file endpoint ([#138](https://github.com/instill-ai/artifact-backend/issues/138)) ([107dcf0](https://github.com/instill-ai/artifact-backend/commit/107dcf062bd0ccca1602b34cf3fddb80599c4006))
* **conversion:** add fallback for unavailable model ([#156](https://github.com/instill-ai/artifact-backend/issues/156)) ([e620fc6](https://github.com/instill-ai/artifact-backend/commit/e620fc615609bb5cb3020774f9e99b3c2aeb5823))
* **conversion:** support instill model in doc conversion process ([#154](https://github.com/instill-ai/artifact-backend/issues/154)) ([4b28b57](https://github.com/instill-ai/artifact-backend/commit/4b28b577ab31aee670e83ead33cca0cc9fe89cfe))
* **index:** implement updated indexing logic ([#146](https://github.com/instill-ai/artifact-backend/issues/146)) ([7f15cc4](https://github.com/instill-ai/artifact-backend/commit/7f15cc4d1afe7e5314bae79af7de3f59e389cb90))
* **init:** create the preset pipelines programmatically ([#142](https://github.com/instill-ai/artifact-backend/issues/142)) ([87ec719](https://github.com/instill-ai/artifact-backend/commit/87ec7195c91a43fa4e872691a21fa00e312d6211))
* **milvus:** support filtered search for metadata ([#150](https://github.com/instill-ai/artifact-backend/issues/150)) ([da0b848](https://github.com/instill-ai/artifact-backend/commit/da0b848b1d6829c1e9d00509aabc7a20d8746fb6))
* **minio:** add agent header to presigned minio URLs ([#153](https://github.com/instill-ai/artifact-backend/issues/153)) ([12eb600](https://github.com/instill-ai/artifact-backend/commit/12eb600bdfc9222a047a93bb3fa0933238f68fc2))
* **minio:** add service name and version to MinIO requests ([#149](https://github.com/instill-ai/artifact-backend/issues/149)) ([4da9f2f](https://github.com/instill-ai/artifact-backend/commit/4da9f2f6c9eb6e296d50b619f1d173e6f6729c85))
* **minio:** emit MinIO audit logs in service logs ([#144](https://github.com/instill-ai/artifact-backend/issues/144)) ([16f2102](https://github.com/instill-ai/artifact-backend/commit/16f21020a2368ed78b8877c9cf6c0a290ff0cc73))


### Bug Fixes

* **artifact:** fix advanced index pipeline ([#135](https://github.com/instill-ai/artifact-backend/issues/135)) ([be3010e](https://github.com/instill-ai/artifact-backend/commit/be3010ea106076f19b3d6895d5b3c11efad905d3))
* **artifact:** upgrade net package to fix vulnerability ([#140](https://github.com/instill-ai/artifact-backend/issues/140)) ([9fe2769](https://github.com/instill-ai/artifact-backend/commit/9fe2769842512b7940c164eea1af18d6f0c5e5e0))
* **blob:** fix concurrently create url issue ([#137](https://github.com/instill-ai/artifact-backend/issues/137)) ([bbce00f](https://github.com/instill-ai/artifact-backend/commit/bbce00f05c0411c17c9c6a87063b413f841fe2e1))

## [0.22.0-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.21.0-alpha...v0.22.0-alpha) (2024-11-30)


### Features

* **artifact:** implement fast indexing for temporary catalog ([#134](https://github.com/instill-ai/artifact-backend/issues/134)) ([f7dd6d5](https://github.com/instill-ai/artifact-backend/commit/f7dd6d5ab60f03ff92637e31ea7aa7ae5261728f))
* **artifact:** implement search chunks and sources ([#133](https://github.com/instill-ai/artifact-backend/issues/133)) ([8f1c966](https://github.com/instill-ai/artifact-backend/commit/8f1c966a925de6e5528a68b74dfa60ce853735f5))
* **artifact:** support ephemeral catalog ([#131](https://github.com/instill-ai/artifact-backend/issues/131)) ([4ed0edd](https://github.com/instill-ai/artifact-backend/commit/4ed0edd779e8b593b129269b7774994aba11b420))

## [0.21.0-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.20.0-alpha...v0.21.0-alpha) (2024-11-14)


### Features

* **artifact:** adopt the advanced converting pipeline ([#127](https://github.com/instill-ai/artifact-backend/issues/127)) ([b5be01b](https://github.com/instill-ai/artifact-backend/commit/b5be01bca87700aa923f31d8250f6a218ad36f91))
* **blob:** add domain in objecturl ([#130](https://github.com/instill-ai/artifact-backend/issues/130)) ([63c24df](https://github.com/instill-ai/artifact-backend/commit/63c24df9d900d4f15e0e2e42e5e00a88602130be))


### Bug Fixes

* **artifact:** update htlm file process ([#129](https://github.com/instill-ai/artifact-backend/issues/129)) ([279c241](https://github.com/instill-ai/artifact-backend/commit/279c241668b9040ceb30104fc2b692062ccce9ed))

## [0.20.0-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.19.0-alpha...v0.20.0-alpha) (2024-11-05)


### Features

* **catalog:** allow external service to store file metadata ([#123](https://github.com/instill-ai/artifact-backend/issues/123)) ([6c97540](https://github.com/instill-ai/artifact-backend/commit/6c975408b6aabfb0c49aed0fb33e40150bbd9615))
* revert the converting pipeline ([#126](https://github.com/instill-ai/artifact-backend/issues/126)) ([b481227](https://github.com/instill-ai/artifact-backend/commit/b4812270457b5380c03ac3541f9fe198d64be852))


### Performance Improvements

* **catalog:** enhance the stability of embeddings saving ([#125](https://github.com/instill-ai/artifact-backend/issues/125)) ([17e6868](https://github.com/instill-ai/artifact-backend/commit/17e68684c99798d27ecc46496f7cbfcffa4b86b1))

## [0.19.0-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.18.0-alpha...v0.19.0-alpha) (2024-10-29)


### Features

* **blob:** implement GetUploadURL in service lib ([#119](https://github.com/instill-ai/artifact-backend/issues/119)) ([931b1ca](https://github.com/instill-ai/artifact-backend/commit/931b1caab47256531ec3e7e83812777c9484f7f3))
* **blob:** implement object and object url repository ([#117](https://github.com/instill-ai/artifact-backend/issues/117)) ([4107ad1](https://github.com/instill-ai/artifact-backend/commit/4107ad18f8aedf307e81f7aaf5c282691fe32f51))
* **blob:** provide blob url endpoint ([#121](https://github.com/instill-ai/artifact-backend/issues/121)) ([860b539](https://github.com/instill-ai/artifact-backend/commit/860b539da376c5ab15300837673a8645e525f07e))
* **blob:** provide the upload object url endpoint ([#120](https://github.com/instill-ai/artifact-backend/issues/120)) ([e207a2f](https://github.com/instill-ai/artifact-backend/commit/e207a2f0fd3453a6a60e03de0d00be3e5d4b2e7c))
* **catalog:** use advanced converting pipleine ([#122](https://github.com/instill-ai/artifact-backend/issues/122)) ([2332507](https://github.com/instill-ai/artifact-backend/commit/2332507073cb37323f52ba7a2496c748b1167d85))

## [0.18.0-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.17.0-alpha...v0.18.0-alpha) (2024-10-15)


### Features

* add check on text prompt ([#115](https://github.com/instill-ai/artifact-backend/issues/115)) ([b612e82](https://github.com/instill-ai/artifact-backend/commit/b612e8246e869e511ded8315b77ca695a189db69))
* add pipeline metadata in error message ([#112](https://github.com/instill-ai/artifact-backend/issues/112)) ([d0a5875](https://github.com/instill-ai/artifact-backend/commit/d0a58755608dad3ab339e613b5fb58efe16ffd50))


### Bug Fixes

* **artifact:** fix minio "get file by path" ([#116](https://github.com/instill-ai/artifact-backend/issues/116)) ([25cf426](https://github.com/instill-ai/artifact-backend/commit/25cf4263c08699a266c1162f2a9c690a732106cd))
* ignore empty chunk from  chunk pipeline ([#114](https://github.com/instill-ai/artifact-backend/issues/114)) ([983374f](https://github.com/instill-ai/artifact-backend/commit/983374f732e6e3276128e1ff2c7e658e830e79ce))

## [0.17.0-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.16.1-alpha...v0.17.0-alpha) (2024-10-08)


### Features

* **artifact:** add retry in minIO and milvus ([#109](https://github.com/instill-ai/artifact-backend/issues/109)) ([c5bbf5f](https://github.com/instill-ai/artifact-backend/commit/c5bbf5f146fb843ea4a364f04c1b0e059b3a68b4))


### Bug Fixes

* **artifact:** add simple rate limiting to use minIO ([#111](https://github.com/instill-ai/artifact-backend/issues/111)) ([25043e0](https://github.com/instill-ai/artifact-backend/commit/25043e0f841f4618b569cdc892c20d5d7ca19006))

## [0.16.1-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.16.0-alpha...v0.16.1-alpha) (2024-10-03)


### Bug Fixes

* increase max payload size ([#107](https://github.com/instill-ai/artifact-backend/issues/107)) ([61b2187](https://github.com/instill-ai/artifact-backend/commit/61b21876518681c71a7f903d5b9361db03df83cf))

## [0.16.0-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.15.0-alpha...v0.16.0-alpha) (2024-10-03)


### Features

* **catalog:** add chunk metadata in api ([#104](https://github.com/instill-ai/artifact-backend/issues/104)) ([ebef89b](https://github.com/instill-ai/artifact-backend/commit/ebef89b78ada8867bab7e5376d9ea933b7980071))


### Bug Fixes

* **artifact:** increase grpc message size ([#106](https://github.com/instill-ai/artifact-backend/issues/106)) ([bceee68](https://github.com/instill-ai/artifact-backend/commit/bceee68c1f7cb1347376132255b358e4bd7d7383))

## [0.15.0-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.14.0-alpha...v0.15.0-alpha) (2024-09-24)


### Features

* **artifact:** add response from pipeline when calling fails ([#100](https://github.com/instill-ai/artifact-backend/issues/100)) ([0eef0cc](https://github.com/instill-ai/artifact-backend/commit/0eef0ccf76e1730b3f6891c8710ba46e067a296b))
* **artifact:** support csv file type upload and some improvements ([#101](https://github.com/instill-ai/artifact-backend/issues/101)) ([ac2eb86](https://github.com/instill-ai/artifact-backend/commit/ac2eb86a420796fffca26de4222ff8da642b40a5))
* **artifact:** support xls ([#99](https://github.com/instill-ai/artifact-backend/issues/99)) ([ae30e81](https://github.com/instill-ai/artifact-backend/commit/ae30e816132e5d7589b30411b67529457f0610bd))
* **catalog:** add catalog uid in list catalog api ([289fcfa](https://github.com/instill-ai/artifact-backend/commit/289fcfa6bc29224834f4ef55213b567f0c1c42a7))


### Bug Fixes

* **artifact:** fix retry file process ([#97](https://github.com/instill-ai/artifact-backend/issues/97)) ([97ff707](https://github.com/instill-ai/artifact-backend/commit/97ff7076a33f4b7f38042a996be2e99ad4f32a00))


### Miscellaneous Chores

* release 1.15.0 ([4ac1378](https://github.com/instill-ai/artifact-backend/commit/4ac13789dc390425df8d24e76ecc1226a14ca78f))
* release 1.15.0-alpha ([4770a4a](https://github.com/instill-ai/artifact-backend/commit/4770a4a8d7bc9b5a94c613aabb57cd87cd77321c))
* release v0.15.0-alpha ([636702d](https://github.com/instill-ai/artifact-backend/commit/636702d63ca030784dacb6358bce7ec878f7996f))

## [0.14.0-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.13.2-alpha...v0.14.0-alpha) (2024-09-13)


### Features

* add CreateRepositoryTag endpoint ([#8](https://github.com/instill-ai/artifact-backend/issues/8)) ([61bc325](https://github.com/instill-ai/artifact-backend/commit/61bc32515f6bb6af066575e93913236b6ffbf666))
* aggregate tag list with database info ([#7](https://github.com/instill-ai/artifact-backend/issues/7)) ([6cc2d8d](https://github.com/instill-ai/artifact-backend/commit/6cc2d8d7130c47ca7448d37879dede1bd5bbb2ec))
* **artifact:** add minIO retry and file deletion ([#89](https://github.com/instill-ai/artifact-backend/issues/89)) ([8f391f0](https://github.com/instill-ai/artifact-backend/commit/8f391f01b73e67fb3d3646865545ad48f8a3cb9f))
* **artifact:** update protogen-go ([c8d41a7](https://github.com/instill-ai/artifact-backend/commit/c8d41a74725dc9b1f14385a156d7d24e7b89ba07))
* **artifact:** use retrievable to decide if chunk can be return ([#54](https://github.com/instill-ai/artifact-backend/issues/54)) ([bde3e85](https://github.com/instill-ai/artifact-backend/commit/bde3e85903b8be9c90e422bb6f3bc20f1c96fb43))
* **catalog:** add file catalog api ([#73](https://github.com/instill-ai/artifact-backend/issues/73)) ([c30317f](https://github.com/instill-ai/artifact-backend/commit/c30317fb9d57d695ca2330d52154e4e75874eb0c))
* **catalog:** check the user tier for catalog limit ([#70](https://github.com/instill-ai/artifact-backend/issues/70)) ([d35a96f](https://github.com/instill-ai/artifact-backend/commit/d35a96ff6e738ef511d2ecacf203e32cf7c87aa8))
* **catalog:** implement conversation and message api ([#77](https://github.com/instill-ai/artifact-backend/issues/77)) ([e02b1f1](https://github.com/instill-ai/artifact-backend/commit/e02b1f1e80f9f34993860ef48a727bcfb3df2a56))
* **catalog:** order asc in create time ([#80](https://github.com/instill-ai/artifact-backend/issues/80)) ([98348e9](https://github.com/instill-ai/artifact-backend/commit/98348e93fa0b96e64a849f0242503373efddc0ec))
* **catalog:** rename endpoint from kb to catalog ([#56](https://github.com/instill-ai/artifact-backend/issues/56)) ([c8e543d](https://github.com/instill-ai/artifact-backend/commit/c8e543dd8ba9204c8df13f65184009cb7640c965))
* **catalog:** sort the chunk ([#74](https://github.com/instill-ai/artifact-backend/issues/74)) ([c434cbd](https://github.com/instill-ai/artifact-backend/commit/c434cbd855394dad6a58022629fa5b53a7e7796e))
* **catalog:** support concurrent text to embedding process ([#85](https://github.com/instill-ai/artifact-backend/issues/85)) ([12d313c](https://github.com/instill-ai/artifact-backend/commit/12d313ce6d4d6d55c74e7aa053c110a67b21ece6))
* **catalog:** support different file-to-embedding process ([#69](https://github.com/instill-ai/artifact-backend/issues/69)) ([7f40dc1](https://github.com/instill-ai/artifact-backend/commit/7f40dc1c1083ffcbef8c09c5d9e407c5c8498ce8))
* **catalog:** support more file type to uplaod ([#67](https://github.com/instill-ai/artifact-backend/issues/67)) ([2d3c705](https://github.com/instill-ai/artifact-backend/commit/2d3c705428b73354c986509f288afae883c4cb44))
* **catalog:** support originalData return ([#87](https://github.com/instill-ai/artifact-backend/issues/87)) ([eb0c7fd](https://github.com/instill-ai/artifact-backend/commit/eb0c7fd989dfea056037c6a083503e7a22c4fc04))
* **catalog:** support question answering ([#71](https://github.com/instill-ai/artifact-backend/issues/71)) ([a540c93](https://github.com/instill-ai/artifact-backend/commit/a540c9321b91645704546d084fc89f237fd12e26))
* **catalog:** support xlsx ([#79](https://github.com/instill-ai/artifact-backend/issues/79)) ([f1e2505](https://github.com/instill-ai/artifact-backend/commit/f1e25055ca0dd9a4015ab7b903ba910d2279d898))
* **catalog:** update the pipeline that ask endpoint use ([#83](https://github.com/instill-ai/artifact-backend/issues/83)) ([b5bbc75](https://github.com/instill-ai/artifact-backend/commit/b5bbc7520837999fbc1de6aa93b6aa99e244f44c))
* **catalog:** update the proto-go ([#82](https://github.com/instill-ai/artifact-backend/issues/82)) ([94fa708](https://github.com/instill-ai/artifact-backend/commit/94fa708b0ff275cbb97877ad7d82dd9fa31f1c52))
* **catelog:** make topK default 5 ([#62](https://github.com/instill-ai/artifact-backend/issues/62)) ([02259e1](https://github.com/instill-ai/artifact-backend/commit/02259e18eceaedf5996a1a7626c057007b9cedeb))
* create artifact database if it does not exist ([#4](https://github.com/instill-ai/artifact-backend/issues/4)) ([787a4ad](https://github.com/instill-ai/artifact-backend/commit/787a4add7d9d10c41bfb6fd3d51a3c8dbd15d836))
* fetch repository tags from registry ([#6](https://github.com/instill-ai/artifact-backend/issues/6)) ([3568735](https://github.com/instill-ai/artifact-backend/commit/3568735ee87a8d263923f6e4b5816d888517ea98))
* **kb:** add acl and update pipeline ([#49](https://github.com/instill-ai/artifact-backend/issues/49)) ([bf7feea](https://github.com/instill-ai/artifact-backend/commit/bf7feea24a3dde4f43bc07cc8690814c2ebe8e3b))
* **kb:** add check and clear message in openfga init ([#52](https://github.com/instill-ai/artifact-backend/issues/52)) ([0a11838](https://github.com/instill-ai/artifact-backend/commit/0a118385e1fc97a6fe06dd92716818d5000253c4))
* **kb:** add file metadata ([#37](https://github.com/instill-ai/artifact-backend/issues/37)) ([51113ce](https://github.com/instill-ai/artifact-backend/commit/51113ce9e87fd9c03609b1b2acc3e55f460e723f))
* **kb:** add owner_id parameter in knowledge base endpoint ([#27](https://github.com/instill-ai/artifact-backend/issues/27)) ([e85020b](https://github.com/instill-ai/artifact-backend/commit/e85020bcc929a4906489a156047a45ad6b205715))
* **kb:** add resource limit ([#43](https://github.com/instill-ai/artifact-backend/issues/43)) ([5f61f44](https://github.com/instill-ai/artifact-backend/commit/5f61f44be8b10f55cc1bee24a8db1eb3bb5dbb08))
* **kb:** add some kb metadata ([#36](https://github.com/instill-ai/artifact-backend/issues/36)) ([0e42ff4](https://github.com/instill-ai/artifact-backend/commit/0e42ff4b86eae4f3b8c5b1ac45d6fe7e70eb5160))
* **KB:** chunk catalog api ([#39](https://github.com/instill-ai/artifact-backend/issues/39)) ([71a3996](https://github.com/instill-ai/artifact-backend/commit/71a3996d91e959448f9ee4e310b49b2d53682445))
* **kb:** dealing with failure in file-to-embedding process ([#47](https://github.com/instill-ai/artifact-backend/issues/47)) ([0448a69](https://github.com/instill-ai/artifact-backend/commit/0448a69428355a6055c785dd6ac4077dee125453))
* **KB:** file-to-embedding worker pool ([#32](https://github.com/instill-ai/artifact-backend/issues/32)) ([5409db3](https://github.com/instill-ai/artifact-backend/commit/5409db365ff6b85c35e7f821c290a9776f9a4d7f))
* **kb:** knowldge base crud endpoints ([#19](https://github.com/instill-ai/artifact-backend/issues/19)) ([0e9f32b](https://github.com/instill-ai/artifact-backend/commit/0e9f32bfd05a0c44bae932cc98f9226dd0def4d0))
* **kb:** knowledge base repository done ([#18](https://github.com/instill-ai/artifact-backend/issues/18)) ([f6aeaae](https://github.com/instill-ai/artifact-backend/commit/f6aeaaeaf3838caa9afebf72271f52f28a5e1eff))
* **kb:** retrieval test api ([#41](https://github.com/instill-ai/artifact-backend/issues/41)) ([8b94cc9](https://github.com/instill-ai/artifact-backend/commit/8b94cc9cdf11e69ec424edd01d66b8579b703f80))
* **kb:** support knowledge base file related api ([#23](https://github.com/instill-ai/artifact-backend/issues/23)) ([3912028](https://github.com/instill-ai/artifact-backend/commit/3912028117c00fd4bdac7d5a955f5b7245423954))
* **kb:** update preset's pipeline ([2751023](https://github.com/instill-ai/artifact-backend/commit/27510239976aadebd79f4f91ccfcd48c9b7d0f55))
* **kb:** using preset's pipeline for file-to-embedding worker ([#45](https://github.com/instill-ai/artifact-backend/issues/45)) ([8c57ad1](https://github.com/instill-ai/artifact-backend/commit/8c57ad119db21ec073b53d0440ed17376ae96fed))
* **registry:** support image deletion ([#29](https://github.com/instill-ai/artifact-backend/issues/29)) ([fe818da](https://github.com/instill-ai/artifact-backend/commit/fe818dac1772671573aa71dd0a97308f038a63fd))
* use camelCase for HTTP body ([#22](https://github.com/instill-ai/artifact-backend/issues/22)) ([5d0fc2f](https://github.com/instill-ai/artifact-backend/commit/5d0fc2f7478da908a27cadc74f8e04377eaa74ff))
* use dind in Dockerfile ([#14](https://github.com/instill-ai/artifact-backend/issues/14)) ([d95aa68](https://github.com/instill-ai/artifact-backend/commit/d95aa68c77d6d41112ea00686c54ee36b184b154))


### Bug Fixes

* add /bin/sh to Dockerfile ([#10](https://github.com/instill-ai/artifact-backend/issues/10)) ([7df1dd3](https://github.com/instill-ai/artifact-backend/commit/7df1dd36e45d36bbeee7804ad14d451aa8c2cda4))
* **artifact:** fix ctx issue ([#91](https://github.com/instill-ai/artifact-backend/issues/91)) ([1c61d69](https://github.com/instill-ai/artifact-backend/commit/1c61d69c9b91ca27a3a321170aaee47f9f28a593))
* **artifact:** improve catalog deletion slow issue ([#93](https://github.com/instill-ai/artifact-backend/issues/93)) ([0bbf3f2](https://github.com/instill-ai/artifact-backend/commit/0bbf3f2a72b2ffce7a5ed87d049d26c673fd2af2))
* **artifact:** use correct version of proto ([#84](https://github.com/instill-ai/artifact-backend/issues/84)) ([cba7e06](https://github.com/instill-ai/artifact-backend/commit/cba7e06851c6bf7ae96924428de93b0c9622ee56))
* **catalog:** add requester when calling pipeline ([#72](https://github.com/instill-ai/artifact-backend/issues/72)) ([0880d91](https://github.com/instill-ai/artifact-backend/commit/0880d91f449ce35ca7a8f0d51c13206e9201f2c1))
* **catalog:** call embedding with max 32 size batch ([#60](https://github.com/instill-ai/artifact-backend/issues/60)) ([e6b25ec](https://github.com/instill-ai/artifact-backend/commit/e6b25ecd02655c351e78b25086ce5f866f4ab134))
* **catalog:** fix the catalog permission issue ([#75](https://github.com/instill-ai/artifact-backend/issues/75)) ([2db2ce3](https://github.com/instill-ai/artifact-backend/commit/2db2ce3563dbe5d6eee72f0b69ae40f5766d3fe4))
* **catalog:** fix topk to topK ([#65](https://github.com/instill-ai/artifact-backend/issues/65)) ([5ff89fe](https://github.com/instill-ai/artifact-backend/commit/5ff89fee57b2983c746ddde9d5b4c8a75cc6c78b))
* **catalog:** list file api's page token ([#64](https://github.com/instill-ai/artifact-backend/issues/64)) ([ac56be0](https://github.com/instill-ai/artifact-backend/commit/ac56be0cbd1155bfad8da04946c2a90280ffb865))
* **catalog:** max 3 catalog per namespace ([#58](https://github.com/instill-ai/artifact-backend/issues/58)) ([786790c](https://github.com/instill-ai/artifact-backend/commit/786790cbd57afb4a93d331fbcecab4c60a243af5))
* **catalog:** when delete catalog and file, also delete the artifact ([#61](https://github.com/instill-ai/artifact-backend/issues/61)) ([cf6ecc3](https://github.com/instill-ai/artifact-backend/commit/cf6ecc3132dc3bcfeedfe6a72bdba54d94090cc5))
* expose private API on private port ([#9](https://github.com/instill-ai/artifact-backend/issues/9)) ([9ef4b03](https://github.com/instill-ai/artifact-backend/commit/9ef4b0306c2ef3404d435f3b30f56bf96422c32b))
* **kb:** empty similar chunks ([#55](https://github.com/instill-ai/artifact-backend/issues/55)) ([d1d5345](https://github.com/instill-ai/artifact-backend/commit/d1d53451a77065b95b560edbc5f34af05a571b40))
* **kb:** fix db migration error ([#53](https://github.com/instill-ai/artifact-backend/issues/53)) ([7cc65b7](https://github.com/instill-ai/artifact-backend/commit/7cc65b7e846a096c1e1aae59cc41c69318a96781))
* **kb:** fixed some bugs in file-to-embedding process ([#35](https://github.com/instill-ai/artifact-backend/issues/35)) ([703bb0b](https://github.com/instill-ai/artifact-backend/commit/703bb0b23288962891922e07f6295d230dd9b780))
* **kb:** get owner uid ([#26](https://github.com/instill-ai/artifact-backend/issues/26)) ([b1d8ac5](https://github.com/instill-ai/artifact-backend/commit/b1d8ac5fb21102ce7808bf08e541dcda000fa287))
* **kb:** issue of chunking ([#34](https://github.com/instill-ai/artifact-backend/issues/34)) ([66307c7](https://github.com/instill-ai/artifact-backend/commit/66307c75035ff96a8cb3bfd139bff2d269d06213))
* **kb:** similar chunk search by prompt text ([#46](https://github.com/instill-ai/artifact-backend/issues/46)) ([265f101](https://github.com/instill-ai/artifact-backend/commit/265f101793ccfd43dbcaa9a2b53de2023b18b6df))
* **kb:** use correct kb uid in chunk similarity search ([#44](https://github.com/instill-ai/artifact-backend/issues/44)) ([e76aafa](https://github.com/instill-ai/artifact-backend/commit/e76aafab951b3a73d08bb985190af4d0c1aa25ed))
* return pagination in tag list endpoint ([#17](https://github.com/instill-ai/artifact-backend/issues/17)) ([72bc47a](https://github.com/instill-ai/artifact-backend/commit/72bc47a7e1de800510d30f1c64cd6dcd98ab2162))

## [0.13.2-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.13.1-alpha...v0.13.2-alpha) (2024-09-13)


### Bug Fixes

* **artifact:** improve catalog deletion slow issue ([#93](https://github.com/instill-ai/artifact-backend/issues/93)) ([0bbf3f2](https://github.com/instill-ai/artifact-backend/commit/0bbf3f2a72b2ffce7a5ed87d049d26c673fd2af2))

## [0.13.1-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.13.0-alpha...v0.13.1-alpha) (2024-09-12)


### Bug Fixes

* **artifact:** fix ctx issue ([#91](https://github.com/instill-ai/artifact-backend/issues/91)) ([1c61d69](https://github.com/instill-ai/artifact-backend/commit/1c61d69c9b91ca27a3a321170aaee47f9f28a593))

## [0.13.0-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.12.0-alpha...v0.13.0-alpha) (2024-09-12)


### Features

* **artifact:** add minIO retry and file deletion ([#89](https://github.com/instill-ai/artifact-backend/issues/89)) ([8f391f0](https://github.com/instill-ai/artifact-backend/commit/8f391f01b73e67fb3d3646865545ad48f8a3cb9f))

## [0.12.0-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.11.0-alpha...v0.12.0-alpha) (2024-09-04)


### Features

* **catalog:** support concurrent text to embedding process ([#85](https://github.com/instill-ai/artifact-backend/issues/85)) ([12d313c](https://github.com/instill-ai/artifact-backend/commit/12d313ce6d4d6d55c74e7aa053c110a67b21ece6))
* **catalog:** support originalData return ([#87](https://github.com/instill-ai/artifact-backend/issues/87)) ([eb0c7fd](https://github.com/instill-ai/artifact-backend/commit/eb0c7fd989dfea056037c6a083503e7a22c4fc04))

## [0.11.0-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.10.1-alpha...v0.11.0-alpha) (2024-08-26)


### Features

* **catalog:** implement conversation and message api ([#77](https://github.com/instill-ai/artifact-backend/issues/77)) ([e02b1f1](https://github.com/instill-ai/artifact-backend/commit/e02b1f1e80f9f34993860ef48a727bcfb3df2a56))
* **catalog:** order asc in create time ([#80](https://github.com/instill-ai/artifact-backend/issues/80)) ([98348e9](https://github.com/instill-ai/artifact-backend/commit/98348e93fa0b96e64a849f0242503373efddc0ec))
* **catalog:** support xlsx ([#79](https://github.com/instill-ai/artifact-backend/issues/79)) ([f1e2505](https://github.com/instill-ai/artifact-backend/commit/f1e25055ca0dd9a4015ab7b903ba910d2279d898))
* **catalog:** update the pipeline that ask endpoint use ([#83](https://github.com/instill-ai/artifact-backend/issues/83)) ([b5bbc75](https://github.com/instill-ai/artifact-backend/commit/b5bbc7520837999fbc1de6aa93b6aa99e244f44c))
* **catalog:** update the proto-go ([#82](https://github.com/instill-ai/artifact-backend/issues/82)) ([94fa708](https://github.com/instill-ai/artifact-backend/commit/94fa708b0ff275cbb97877ad7d82dd9fa31f1c52))


### Bug Fixes

* **artifact:** use correct version of proto ([#84](https://github.com/instill-ai/artifact-backend/issues/84)) ([cba7e06](https://github.com/instill-ai/artifact-backend/commit/cba7e06851c6bf7ae96924428de93b0c9622ee56))

## [0.10.1-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.10.0-alpha...v0.10.1-alpha) (2024-08-14)


### Bug Fixes

* **catalog:** fix the catalog permission issue ([#75](https://github.com/instill-ai/artifact-backend/issues/75)) ([2db2ce3](https://github.com/instill-ai/artifact-backend/commit/2db2ce3563dbe5d6eee72f0b69ae40f5766d3fe4))

## [0.10.0-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.9.1-alpha...v0.10.0-alpha) (2024-08-12)


### Features

* **catalog:** add file catalog api ([#73](https://github.com/instill-ai/artifact-backend/issues/73)) ([c30317f](https://github.com/instill-ai/artifact-backend/commit/c30317fb9d57d695ca2330d52154e4e75874eb0c))
* **catalog:** check the user tier for catalog limit ([#70](https://github.com/instill-ai/artifact-backend/issues/70)) ([d35a96f](https://github.com/instill-ai/artifact-backend/commit/d35a96ff6e738ef511d2ecacf203e32cf7c87aa8))
* **catalog:** sort the chunk ([#74](https://github.com/instill-ai/artifact-backend/issues/74)) ([c434cbd](https://github.com/instill-ai/artifact-backend/commit/c434cbd855394dad6a58022629fa5b53a7e7796e))
* **catalog:** support different file-to-embedding process ([#69](https://github.com/instill-ai/artifact-backend/issues/69)) ([7f40dc1](https://github.com/instill-ai/artifact-backend/commit/7f40dc1c1083ffcbef8c09c5d9e407c5c8498ce8))
* **catalog:** support more file type to uplaod ([#67](https://github.com/instill-ai/artifact-backend/issues/67)) ([2d3c705](https://github.com/instill-ai/artifact-backend/commit/2d3c705428b73354c986509f288afae883c4cb44))
* **catalog:** support question answering ([#71](https://github.com/instill-ai/artifact-backend/issues/71)) ([a540c93](https://github.com/instill-ai/artifact-backend/commit/a540c9321b91645704546d084fc89f237fd12e26))


### Bug Fixes

* **catalog:** add requester when calling pipeline ([#72](https://github.com/instill-ai/artifact-backend/issues/72)) ([0880d91](https://github.com/instill-ai/artifact-backend/commit/0880d91f449ce35ca7a8f0d51c13206e9201f2c1))

## [0.9.1-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.9.0-alpha...v0.9.1-alpha) (2024-08-06)


### Bug Fixes

* **catalog:** fix topk to topK ([#65](https://github.com/instill-ai/artifact-backend/issues/65)) ([5ff89fe](https://github.com/instill-ai/artifact-backend/commit/5ff89fee57b2983c746ddde9d5b4c8a75cc6c78b))

## [0.9.0-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.8.1-alpha...v0.9.0-alpha) (2024-08-05)


### Features

* **catelog:** make topK default 5 ([#62](https://github.com/instill-ai/artifact-backend/issues/62)) ([02259e1](https://github.com/instill-ai/artifact-backend/commit/02259e18eceaedf5996a1a7626c057007b9cedeb))


### Bug Fixes

* **catalog:** list file api's page token ([#64](https://github.com/instill-ai/artifact-backend/issues/64)) ([ac56be0](https://github.com/instill-ai/artifact-backend/commit/ac56be0cbd1155bfad8da04946c2a90280ffb865))

## [0.8.1-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.8.0-alpha...v0.8.1-alpha) (2024-08-01)


### Bug Fixes

* **catalog:** call embedding with max 32 size batch ([#60](https://github.com/instill-ai/artifact-backend/issues/60)) ([e6b25ec](https://github.com/instill-ai/artifact-backend/commit/e6b25ecd02655c351e78b25086ce5f866f4ab134))
* **catalog:** max 3 catalog per namespace ([#58](https://github.com/instill-ai/artifact-backend/issues/58)) ([786790c](https://github.com/instill-ai/artifact-backend/commit/786790cbd57afb4a93d331fbcecab4c60a243af5))
* **catalog:** when delete catalog and file, also delete the artifact ([#61](https://github.com/instill-ai/artifact-backend/issues/61)) ([cf6ecc3](https://github.com/instill-ai/artifact-backend/commit/cf6ecc3132dc3bcfeedfe6a72bdba54d94090cc5))

## [0.8.0-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.7.0-alpha...v0.8.0-alpha) (2024-08-01)


### Features

* **catalog:** rename endpoint from kb to catalog ([#56](https://github.com/instill-ai/artifact-backend/issues/56)) ([c8e543d](https://github.com/instill-ai/artifact-backend/commit/c8e543dd8ba9204c8df13f65184009cb7640c965))

## [0.7.0-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.6.0-alpha...v0.7.0-alpha) (2024-07-30)


### Features

* **artifact:** update protogen-go ([c8d41a7](https://github.com/instill-ai/artifact-backend/commit/c8d41a74725dc9b1f14385a156d7d24e7b89ba07))
* **artifact:** use retrievable to decide if chunk can be return ([#54](https://github.com/instill-ai/artifact-backend/issues/54)) ([bde3e85](https://github.com/instill-ai/artifact-backend/commit/bde3e85903b8be9c90e422bb6f3bc20f1c96fb43))
* **kb:** add acl and update pipeline ([#49](https://github.com/instill-ai/artifact-backend/issues/49)) ([bf7feea](https://github.com/instill-ai/artifact-backend/commit/bf7feea24a3dde4f43bc07cc8690814c2ebe8e3b))
* **kb:** add check and clear message in openfga init ([#52](https://github.com/instill-ai/artifact-backend/issues/52)) ([0a11838](https://github.com/instill-ai/artifact-backend/commit/0a118385e1fc97a6fe06dd92716818d5000253c4))
* **kb:** add resource limit ([#43](https://github.com/instill-ai/artifact-backend/issues/43)) ([5f61f44](https://github.com/instill-ai/artifact-backend/commit/5f61f44be8b10f55cc1bee24a8db1eb3bb5dbb08))
* **kb:** dealing with failure in file-to-embedding process ([#47](https://github.com/instill-ai/artifact-backend/issues/47)) ([0448a69](https://github.com/instill-ai/artifact-backend/commit/0448a69428355a6055c785dd6ac4077dee125453))
* **kb:** retrieval test api ([#41](https://github.com/instill-ai/artifact-backend/issues/41)) ([8b94cc9](https://github.com/instill-ai/artifact-backend/commit/8b94cc9cdf11e69ec424edd01d66b8579b703f80))
* **kb:** update preset's pipeline ([2751023](https://github.com/instill-ai/artifact-backend/commit/27510239976aadebd79f4f91ccfcd48c9b7d0f55))
* **kb:** using preset's pipeline for file-to-embedding worker ([#45](https://github.com/instill-ai/artifact-backend/issues/45)) ([8c57ad1](https://github.com/instill-ai/artifact-backend/commit/8c57ad119db21ec073b53d0440ed17376ae96fed))


### Bug Fixes

* **kb:** empty similar chunks ([#55](https://github.com/instill-ai/artifact-backend/issues/55)) ([d1d5345](https://github.com/instill-ai/artifact-backend/commit/d1d53451a77065b95b560edbc5f34af05a571b40))
* **kb:** fix db migration error ([#53](https://github.com/instill-ai/artifact-backend/issues/53)) ([7cc65b7](https://github.com/instill-ai/artifact-backend/commit/7cc65b7e846a096c1e1aae59cc41c69318a96781))
* **kb:** similar chunk search by prompt text ([#46](https://github.com/instill-ai/artifact-backend/issues/46)) ([265f101](https://github.com/instill-ai/artifact-backend/commit/265f101793ccfd43dbcaa9a2b53de2023b18b6df))
* **kb:** use correct kb uid in chunk similarity search ([#44](https://github.com/instill-ai/artifact-backend/issues/44)) ([e76aafa](https://github.com/instill-ai/artifact-backend/commit/e76aafab951b3a73d08bb985190af4d0c1aa25ed))

## [0.6.0-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.5.0-alpha...v0.6.0-alpha) (2024-07-15)


### Features

* **kb:** add file metadata ([#37](https://github.com/instill-ai/artifact-backend/issues/37)) ([51113ce](https://github.com/instill-ai/artifact-backend/commit/51113ce9e87fd9c03609b1b2acc3e55f460e723f))
* **kb:** add some kb metadata ([#36](https://github.com/instill-ai/artifact-backend/issues/36)) ([0e42ff4](https://github.com/instill-ai/artifact-backend/commit/0e42ff4b86eae4f3b8c5b1ac45d6fe7e70eb5160))
* **KB:** chunk catalog api ([#39](https://github.com/instill-ai/artifact-backend/issues/39)) ([71a3996](https://github.com/instill-ai/artifact-backend/commit/71a3996d91e959448f9ee4e310b49b2d53682445))
* **KB:** file-to-embedding worker pool ([#32](https://github.com/instill-ai/artifact-backend/issues/32)) ([5409db3](https://github.com/instill-ai/artifact-backend/commit/5409db365ff6b85c35e7f821c290a9776f9a4d7f))


### Bug Fixes

* **kb:** fixed some bugs in file-to-embedding process ([#35](https://github.com/instill-ai/artifact-backend/issues/35)) ([703bb0b](https://github.com/instill-ai/artifact-backend/commit/703bb0b23288962891922e07f6295d230dd9b780))
* **kb:** issue of chunking ([#34](https://github.com/instill-ai/artifact-backend/issues/34)) ([66307c7](https://github.com/instill-ai/artifact-backend/commit/66307c75035ff96a8cb3bfd139bff2d269d06213))

## [0.5.0-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.4.0-alpha...v0.5.0-alpha) (2024-06-25)


### Features

* **kb:** add owner_id parameter in knowledge base endpoint ([#27](https://github.com/instill-ai/artifact-backend/issues/27)) ([e85020b](https://github.com/instill-ai/artifact-backend/commit/e85020bcc929a4906489a156047a45ad6b205715))
* **registry:** support image deletion ([#29](https://github.com/instill-ai/artifact-backend/issues/29)) ([fe818da](https://github.com/instill-ai/artifact-backend/commit/fe818dac1772671573aa71dd0a97308f038a63fd))

## [0.4.0-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.3.0-alpha...v0.4.0-alpha) (2024-06-16)


### Features

* **kb:** support knowledge base file related api ([#23](https://github.com/instill-ai/artifact-backend/issues/23)) ([3912028](https://github.com/instill-ai/artifact-backend/commit/3912028117c00fd4bdac7d5a955f5b7245423954))
* use camelCase for HTTP body ([#22](https://github.com/instill-ai/artifact-backend/issues/22)) ([5d0fc2f](https://github.com/instill-ai/artifact-backend/commit/5d0fc2f7478da908a27cadc74f8e04377eaa74ff))


### Bug Fixes

* **kb:** get owner uid ([#26](https://github.com/instill-ai/artifact-backend/issues/26)) ([b1d8ac5](https://github.com/instill-ai/artifact-backend/commit/b1d8ac5fb21102ce7808bf08e541dcda000fa287))

## [0.3.0-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.2.1-alpha...v0.3.0-alpha) (2024-06-06)


### Features

* **kb:** knowldge base crud endpoints ([#19](https://github.com/instill-ai/artifact-backend/issues/19)) ([0e9f32b](https://github.com/instill-ai/artifact-backend/commit/0e9f32bfd05a0c44bae932cc98f9226dd0def4d0))
* **kb:** knowledge base repository done ([#18](https://github.com/instill-ai/artifact-backend/issues/18)) ([f6aeaae](https://github.com/instill-ai/artifact-backend/commit/f6aeaaeaf3838caa9afebf72271f52f28a5e1eff))
* use dind in Dockerfile ([#14](https://github.com/instill-ai/artifact-backend/issues/14)) ([d95aa68](https://github.com/instill-ai/artifact-backend/commit/d95aa68c77d6d41112ea00686c54ee36b184b154))


### Bug Fixes

* return pagination in tag list endpoint ([#17](https://github.com/instill-ai/artifact-backend/issues/17)) ([72bc47a](https://github.com/instill-ai/artifact-backend/commit/72bc47a7e1de800510d30f1c64cd6dcd98ab2162))

## [0.2.1-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.2.0-alpha...v0.2.1-alpha) (2024-04-05)


### Bug Fixes

* add /bin/sh to Dockerfile ([#10](https://github.com/instill-ai/artifact-backend/issues/10)) ([7df1dd3](https://github.com/instill-ai/artifact-backend/commit/7df1dd36e45d36bbeee7804ad14d451aa8c2cda4))

## [0.2.0-alpha](https://github.com/instill-ai/artifact-backend/compare/v0.1.0-alpha...v0.2.0-alpha) (2024-04-02)


### Features

* add CreateRepositoryTag endpoint ([#8](https://github.com/instill-ai/artifact-backend/issues/8)) ([61bc325](https://github.com/instill-ai/artifact-backend/commit/61bc32515f6bb6af066575e93913236b6ffbf666))
* aggregate tag list with database info ([#7](https://github.com/instill-ai/artifact-backend/issues/7)) ([6cc2d8d](https://github.com/instill-ai/artifact-backend/commit/6cc2d8d7130c47ca7448d37879dede1bd5bbb2ec))
* create artifact database if it does not exist ([#4](https://github.com/instill-ai/artifact-backend/issues/4)) ([787a4ad](https://github.com/instill-ai/artifact-backend/commit/787a4add7d9d10c41bfb6fd3d51a3c8dbd15d836))
* fetch repository tags from registry ([#6](https://github.com/instill-ai/artifact-backend/issues/6)) ([3568735](https://github.com/instill-ai/artifact-backend/commit/3568735ee87a8d263923f6e4b5816d888517ea98))


### Bug Fixes

* expose private API on private port ([#9](https://github.com/instill-ai/artifact-backend/issues/9)) ([9ef4b03](https://github.com/instill-ai/artifact-backend/commit/9ef4b0306c2ef3404d435f3b30f56bf96422c32b))
