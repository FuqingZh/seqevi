# Runtime source correspondence

| Runtime component | Installed artifact | Corresponding source |
| --- | --- | --- |
| SeqEvi 0.3.5 | wheel built from the labeled image revision | `https://github.com/FuqingZh/seqevi/tree/<org.opencontainers.image.revision>` |
| dbCAN 5.2.9 | PyPI wheel SHA-256 `daf39033e9921d116f46a374714f6095b71394eb6438035f1754354d7e20d8d2` | tag `v5.2.9`, commit `614c93f896939042ae5bd574b9c6b971e80803f6`; the verified source archive is bundled as `sources/dbcan-source` |
| DIAMOND 2.1.15 | Linux archive SHA-256 `2c1507fbb32164e861857d606fddf4b92d481174e4015cc50682f51c7b2f978a` | tag `v2.1.15`, commit `5c6b545d2d6eb1b31a5d553f39b3cc65e0aec6ce`; the verified source archive is bundled as `sources/diamond-source` |
| CPython 3.13.11 | `python:3.13.11-slim-bookworm` Linux/amd64 manifest `sha256:ac76900038d8606cc99b413d4ede77bc7152f1e42b94cf5d50d4b80a999652fe` | [CPython 3.13.11 source](https://github.com/python/cpython/tree/v3.13.11); the installed CPython license is bundled as `../licenses/CPython-3.13.11.txt` |
| Python packages | wheels selected by the two hash-locked requirements files | exact versions and upstream project/source URLs are generated into `PYTHON-PACKAGES.tsv`; wheel hashes remain in the lock files |

`inputs.json` is the machine-readable URL and SHA-256 record for the DIAMOND
binary, GPL license texts and corresponding GPL sources. Every one of these
inputs is verified before it enters the image.

The pinned Python image is based on Debian bookworm-slim. BuildKit's attached
final-image SBOM is the authoritative inventory for the unmodified Debian base
package closure; this record does not duplicate every Debian source package.
