/// <reference types="vite/client" />

declare module "*.png" {
  const src: string;
  export default src;
}

interface ImportMetaEnv {
  readonly VITE_BACKEND_URL?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
