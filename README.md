NVIDIA Generative AI Examples
documentation

Introduction
State-of-the-art Generative AI examples that are easy to deploy, test, and extend. All examples run on the high performance NVIDIA CUDA-X software stack and NVIDIA GPUs.

NVIDIA NGC
Generative AI Examples can use models and GPUs from the NVIDIA NGC: AI Development Catalog.

Sign up for a free NGC developer account to access:

GPU-optimized containers used in these examples
Release notes and developer documentation
Retrieval Augmented Generation (RAG)
A RAG pipeline embeds multimodal data -- such as documents, images, and video -- into a database connected to a LLM. RAG lets users chat with their data!

Developer RAG Examples
The developer RAG examples run on a single VM. The examples demonstrate how to combine NVIDIA GPU acceleration with popular LLM programming frameworks using NVIDIA's open source connectors. The examples are easy to deploy with Docker Compose.

Examples support local and remote inference endpoints. If you have a GPU, you can inference locally with TensorRT-LLM. If you don't have a GPU, you can inference and embed remotely with NVIDIA API Catalog endpoints.

Model	Embedding	Framework	Description	Multi-GPU	TRT-LLM	NVIDIA Endpoints	Triton	Vector Database
mixtral_8x7b	nvolveqa_40k	LangChain	NVIDIA API Catalog endpoints chat bot [code, docs]	No	No	Yes	Yes	Milvus or pgvector
llama-2	e5-large-v2	LlamaIndex	Canonical QA Chatbot [code, docs]	Yes	Yes	No	Yes	Milvus or pgvector
llama-2	all-MiniLM-L6-v2	LlamaIndex	Chat bot, GeForce, Windows [repo]	No	Yes	No	No	FAISS
llama-2	nvolveqa_40k	LangChain	Chat bot with query decomposition agent [code, docs]	No	No	Yes	Yes	Milvus or pgvector
mixtral_8x7b	nvolveqa_40k	LangChain	Minimilastic example: RAG with NVIDIA AI Foundation Models [code, README]	No	No	Yes	Yes	FAISS
mixtral_8x7b
Deplot
Neva-22b	nvolveqa_40k	Custom	Chat bot with multimodal data [code, docs]	No	No	Yes	No	Milvus or pvgector
llama-2	e5-large-v2	LlamaIndex	Chat bot with quantized LLM model [docs]	Yes	Yes	No	Yes	Milvus or pgvector
mixtral_8x7b	none	PandasAI	Chat bot with structured data [code, docs]	No	No	Yes	No	none
llama-2	nvolveqa_40k	LangChain	Chat bot with multi-turn conversation [code, docs]	No	No	Yes	No	Milvus or pgvector
Enterprise RAG Examples
The enterprise RAG examples run as microservices distributed across multiple VMs and GPUs. These examples show how to orchestrate RAG pipelines with Kubernetes and deployed with Helm.

Enterprise RAG examples include a Kubernetes operator for LLM lifecycle management. It is compatible with the NVIDIA GPU operator that automates GPU discovery and lifecycle management in a Kubernetes cluster.

Enterprise RAG examples also support local and remote inference with TensorRT-LLM and NVIDIA API Catalog endpoints.

Model	Embedding	Framework	Description	Multi-GPU	Multi-node	TRT-LLM	NVIDIA Endpoints	Triton	Vector Database
llama-2	NV-Embed-QA	LlamaIndex	Chat bot, Kubernetes deployment [README]	No	No	Yes	No	Yes	Milvus
Tools
Example tools and tutorials to enhance LLM development and productivity when using NVIDIA RAG pipelines.

Name	Description	NVIDIA Endpoints
Evaluation	RAG evaluation using synthetic data generation and LLM-as-a-judge [code, docs]	Yes
Observability	Monitoring and debugging RAG pipelines [code, docs]	Yes
Open Source Integrations
These are open source connectors for NVIDIA-hosted and self-hosted API endpoints. These open source connectors are maintained and tested by NVIDIA engineers.

Name	Framework	Chat	Text Embedding	Python	Description
NVIDIA AI Foundation Endpoints	Langchain	Yes	Yes	Yes	Easy access to NVIDIA hosted models. Supports chat, embedding, code generation, steerLM, multimodal, and RAG.
NVIDIA Triton + TensorRT-LLM	Langchain	Yes	Yes	Yes	This connector allows Langchain to remotely interact with a Triton inference server over GRPC or HTTP tfor optimized LLM inference.
NVIDIA Triton Inference Server	LlamaIndex	Yes	Yes	No	Triton inference server provides API access to hosted LLM models over gRPC.
NVIDIA TensorRT-LLM	LlamaIndex	Yes	Yes	No	TensorRT-LLM provides a Python API to build TensorRT engines with state-of-the-art optimizations for LLM inference on NVIDIA GPUs.
Support, Feedback, and Contributing
We're posting these examples on GitHub to support the NVIDIA LLM community and facilitate feedback. We invite contributions via GitHub Issues or pull requests!

Known Issues
Some known issues are identified as TODOs in the Python code.
The datasets provided as part of this project are under a different license for research and evaluation purposes.
This project downloads and installs third-party open source software projects. Review the license terms of these open source projects before use.
