## Requirements

- OpenJDK 17
- A POSIX-compatible shell
- Client and server hosts that can communicate over TCP port `12339`

The Guava runtime JAR used by the project is included in `bin/lib`.

A pre-trained Word2Vec model (such as `GoogleNews-vectors-negative300.bin`)
must be obtained separately to regenerate the `.bin` input files. Models are
not included in this repository. Place the model in `models/` and specify its
path when running `Word2VecSentenceToBin`. The model is not required to run
experiments with the supplied `.bin` files.
