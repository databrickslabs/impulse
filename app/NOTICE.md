Impulse App

Copyright (2026) Databricks, Inc.

This Software includes software developed at Databricks (https://www.databricks.com/) and its use is subject to the LICENSE.txt file at the root of this repository.

---

This NOTICE covers the app's **direct** dependencies, declared in `pyproject.toml` and `frontend/package.json`. They aren't distributed with this repository: they're installed when the app is built and deployed, and `uv.lock` and `frontend/package-lock.json` pin them together with their own dependencies. Dev-only dependencies (build tools, test runners) are marked `dev`; runtime dependencies are marked `runtime`.

| Package | Ecosystem | Type | License | Source |
| --- | --- | --- | --- | --- |
| databricks-sdk | Python | runtime | Apache License 2.0 | [https://databricks-sdk-py.readthedocs.io](https://databricks-sdk-py.readthedocs.io) |
| fastapi | Python | runtime | MIT License | [https://github.com/fastapi/fastapi](https://github.com/fastapi/fastapi) |
| PyYAML | Python | runtime | MIT License | [https://pyyaml.org/](https://pyyaml.org/) |
| uvicorn | Python | runtime | BSD 3-Clause License | [https://uvicorn.dev/](https://uvicorn.dev/) |
| httpx | Python | dev | BSD 3-Clause License | [https://github.com/encode/httpx](https://github.com/encode/httpx) |
| pytest | Python | dev | MIT License | [https://docs.pytest.org/en/latest/](https://docs.pytest.org/en/latest/) |
| lucide-react | npm | runtime | ISC License | [https://github.com/lucide-icons/lucide](https://github.com/lucide-icons/lucide) |
| plotly.js-cartesian-dist-min | npm | runtime | MIT License | [https://github.com/plotly/plotly.js](https://github.com/plotly/plotly.js) |
| react | npm | runtime | MIT License | [https://github.com/react/react](https://github.com/react/react) |
| react-dom | npm | runtime | MIT License | [https://github.com/react/react](https://github.com/react/react) |
| @types/react | npm | dev | MIT License | [https://github.com/DefinitelyTyped/DefinitelyTyped](https://github.com/DefinitelyTyped/DefinitelyTyped) |
| @types/react-dom | npm | dev | MIT License | [https://github.com/DefinitelyTyped/DefinitelyTyped](https://github.com/DefinitelyTyped/DefinitelyTyped) |
| @vitejs/plugin-react | npm | dev | MIT License | [https://github.com/vitejs/vite-plugin-react](https://github.com/vitejs/vite-plugin-react) |
| typescript | npm | dev | Apache License 2.0 | [https://github.com/microsoft/TypeScript](https://github.com/microsoft/TypeScript) |
| vite | npm | dev | MIT License | [https://github.com/vitejs/vite](https://github.com/vitejs/vite) |
