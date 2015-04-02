We prefer to receive code contributions in the form of github pull requests. When you submit a pull request, there are a few things to keep in mind. 

Please ensure code changes

 * **Are Small** Submissions should be for a single feature/bugfix, don't group submissions together
 * **Are Green** All code that is committed to gemfire must pass ./gradlew build. 
 * **Are Discussed** Changes that affect public API or introduce new features must be dicussed on the mailing list. GemFire is a mature product with many active users in production, so new APIs or features need to meet a high standard of quality. If a feature can be developed as an extension of GemFire that may be more desirable - see [[Public APIs|Public_APIs]].
 * **Have a backwards compatible API** All changes to the public API must be backwards compatible, with the exception of removing deprecated features. Removing deprecated features also requires discussion.
 * **Have backwards compatible messaging** GemFire supports rolling upgrades on a live system. Message serialization and disk persistence must be backwards compatible with previous versions. This includes algorithmic changes that don't affect the serialization format. GemFire has a framework for backwards compatible serialization - see [[Managing backward-compatibility|Managing-backward-compatibility]].
 * **Follow Style Guidelines** Code should adhere to the [[Code Style Guide|Code-Style-Guide]].
 * **Have tests** New features must be accompanied by tests. Bug fixes should also include a test for the bug. See [[Writing tests|Writing tests]].
 * **Have few dependencies** Avoid introducing new library dependencies when possible. Licenses for third-party dependencies must conform to [Apache requirements](https://www.apache.org/legal/resolved.html).
 * **Are Safe** Gemfire is a heavily concurrent product. Code changes should be cafefully considered with regard to thread safety and distributed system safety.

It's best to start small with bug fixes or smaller features. Please feel free to discuss things and ask for help on the mailing list if you are interested in constributing. 
