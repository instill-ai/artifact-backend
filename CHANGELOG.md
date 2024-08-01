# Changelog

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
