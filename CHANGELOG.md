# Changelog

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
