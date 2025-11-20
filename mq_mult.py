from __future__ import annotations
from typing import Optional, List, Tuple, Any
import os, re, json, unicodedata

from urllib.parse import quote
import urllib3
import requests
from requests.auth import HTTPBasicAuth

# ===================== Config básica =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def _b(s: Optional[str], default=False) -> bool:
    if s is None: return default
    return str(s).strip().lower() in {"1","true","yes","y","on","t"}

# Prefixos a ignorar na listagem (pode ajustar/ler de env se quiser)
IGNORED_PREFIXES = tuple(p.strip() for p in os.getenv("MQ_IGNORE_PREFIXES", "AMQ,SYSTEM.").split(",") if p.strip())

# ===================== Multi MQ Client =====================
class MQClient:
    """Cliente independente para um endpoint IBM MQ REST."""
    def __init__(self, url_base: str, user: str, password: str, verify_ssl: bool = False, timeout: int = 20):
        self.url_base = url_base.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(user, password)
        self.session.verify = verify_ssl
        self.session.headers.update({"Accept": "application/json"})

    def _get_json(self, path: str):
        try:
            r = self.session.get(f"{self.url_base}{path}", timeout=self.timeout)
            txt = r.text or ""
            try: data = r.json()
            except Exception: data = None
            return r.status_code, txt, data
        except requests.RequestException as e:
            return 0, str(e), None

    def _post_json(self, path: str, payload: dict):
        try:
            r = self.session.post(
                f"{self.url_base}{path}",
                json=payload,
                timeout=self.timeout,
                headers={
                    "ibm-mq-rest-csrf-token": "value",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )
            txt = r.text or ""
            try: data = r.json()
            except Exception: data = None
            return r.status_code, txt, data
        except requests.RequestException as e:
            return 0, str(e), None

    # ---- Endpoints IBM MQ ----
    def listar_qmgrs(self):
        return self._get_json("/ibmmq/rest/v1/admin/qmgr")

    def run_mqsc_display(self, qmgr: str, name: str = "*"):
        payload = {
            "type": "runCommandJSON",
            "command": "display",
            "qualifier": "qlocal",
            "name": name,
            "responseParameters": ["CURDEPTH"],
        }
        return self._post_json(
            f"/ibmmq/rest/v2/admin/action/qmgr/{quote(qmgr, safe='')}/mqsc",
            payload,
        )

# ===================== Ambientes (ajuste as URLs/creds) =====================
MQ_ENVS = {
    # DEV local (padrão)
    "DEV": MQClient(os.getenv("DEV_URL", "https://localhost:9443"),
                    os.getenv("DEV_USER", "mqadmin"),
                    os.getenv("DEV_PASS", "mqadmin"),
                    verify_ssl=_b(os.getenv("DEV_VERIFY_SSL", "false"), False)),
    # Homologação
    "HML": MQClient(os.getenv("HML_URL", "https://mq-hml.sua-empresa.com:9443"),
                    os.getenv("HML_USER", "hmluser"),
                    os.getenv("HML_PASS", "hmlpass"),
                    verify_ssl=_b(os.getenv("HML_VERIFY_SSL", "true"), True)),
    # Produção
    "PRD": MQClient(os.getenv("PRD_URL", "https://mq-prd.sua-empresa.com:9443"),
                    os.getenv("PRD_USER", "mqprod"),
                    os.getenv("PRD_PASS", "SenhaSegura!"),
                    verify_ssl=_b(os.getenv("PRD_VERIFY_SSL", "true"), True)),
}
DEFAULT_ENV = os.getenv("MQ_DEFAULT_ENV", "DEV" if "DEV" in MQ_ENVS else next(iter(MQ_ENVS)))

# ===================== Ordenação e parsing =====================
def _natural_key(s: str):
    return [int(t) if t.isdigit() else (t or "").lower() for t in re.split(r"(\d+)", s or "")]

DEPTH_KEYS = {"curdepth","CURDEPTH","currentdepth","CurrentDepth","qDepth","QDEPTH","depth"}

def _norm_name(x: Any) -> Optional[str]:
    if not isinstance(x, str): return None
    x = unicodedata.normalize("NFKC", x).strip()
    return x or None

def _int_or_none(v: Any) -> Optional[int]:
    try: return int(v)
    except Exception: return None

def _pairs_from_json(data: Any) -> List[tuple[str, Optional[int]]]:
    """
    Extrai lista de pares (queue_name, curdepth|None) de múltiplos esquemas:
    - { "commandResponse": [ { "parameters": {"queue": "...", "CURDEPTH": 0} } ] }
    - { "response": [ { "mqsc": [ {"name": "...", "CURDEPTH": 0}, ... ] } ] }
    - Walk genérico como fallback.
    Aplica dedup (mantém último valor) e filtra prefixos IGNORED_PREFIXES.
    """
    if data is None:
        return []
    out: List[tuple[str, Optional[int]]] = []

    def add(qname, depth):
        qn = _norm_name(qname)
        if qn is not None:
            out.append((qn, _int_or_none(depth)))

    # 1) Esquema atual
    if isinstance(data, dict) and "commandResponse" in data:
        for item in data.get("commandResponse", []):
            if not isinstance(item, dict): continue
            p = item.get("parameters", {})
            if isinstance(p, dict):
                qname = p.get("queue") or p.get("name")
                depth = None
                for k in DEPTH_KEYS:
                    if k in p: depth = p[k]; break
                add(qname, depth)

    # 2) Esquema antigo
    if not out and isinstance(data, dict) and "response" in data:
        for resp in data.get("response", []):
            if not isinstance(resp, dict): continue
            for ent in resp.get("mqsc", []):
                if not isinstance(ent, dict): continue
                qname = ent.get("name") or ent.get("queue")
                depth = None
                for k in DEPTH_KEYS:
                    if k in ent: depth = ent[k]; break
                add(qname, depth)

    # 3) Walk genérico
    if not out:
        def walk(o):
            if isinstance(o, dict):
                qname = o.get("queue") or o.get("name")
                depth = None
                for k in DEPTH_KEYS:
                    if k in o:
                        try: depth = int(o[k])
                        except Exception: depth = None
                        break
                add(qname, depth)
                for v in o.values(): walk(v)
            elif isinstance(o, list):
                for it in o: walk(it)
        walk(data)

    # Dedup mantendo o último valor
    latest: dict[str, Optional[int]] = {}
    for nm, d in out:
        latest[nm] = d if isinstance(d, int) else latest.get(nm, None)

    # Filtro: ignora filas internas
    filtered = [
        (nm, latest[nm])
        for nm in latest.keys()
        if not any(nm.casefold().startswith(p.casefold()) for p in IGNORED_PREFIXES)
    ]
    return filtered

# ===================== API simples (multi-ambiente) =====================
def listar_qmgrs(env: str) -> List[str]:
    client = MQ_ENVS.get(env)
    if not client:
        return []
    code, txt, data = client.listar_qmgrs()
    if code != 200 or not isinstance(data, dict):
        return []
    nomes = [it.get("name") for it in data.get("qmgr", []) if it.get("name")]
    return sorted({n for n in nomes if n})

def listar_filas_pairs(env: str, qmgr: str) -> Tuple[str, List[tuple[str, Optional[int]]], str]:
    """Retorna (status, [(nome, depth)], snippet_erro)"""
    if not qmgr:
        return "Selecione um QMgr.", [], ""
    client = MQ_ENVS.get(env)
    if not client:
        return "Ambiente inválido.", [], ""
    code, txt, data = client.run_mqsc_display(qmgr, "*")
    if code != 200:
        return f"Erro ao listar (HTTP {code}).", [], (txt or "")[:400]
    pairs = _pairs_from_json(data)
    if not pairs:
        snippet = json.dumps(data, indent=2) if isinstance(data, (dict, list)) else (txt or "")[:400]
        return "Nenhuma fila retornada (verifique permissões).", [], (snippet if isinstance(snippet, str) else json.dumps(snippet)[:400])
    # ordena
    pairs = sorted(pairs, key=lambda x: _natural_key(x[0]))
    return "OK", pairs, ""

def consultar_profundidade(env: str, qmgr: str, fila: str) -> Tuple[str, Optional[int]]:
    fila = (fila or "").strip()
    if not fila:
        return "Informe o nome da fila.", None
    client = MQ_ENVS.get(env)
    if not client:
        return "Ambiente inválido.", None
    code, txt, data = client.run_mqsc_display(qmgr, fila)
    if code != 200:
        return f"Erro (HTTP {code})", None
    pairs = _pairs_from_json(data)
    for nm, d in pairs:
        if isinstance(nm, str) and nm.upper() == fila.upper():
            if isinstance(d, int):
                return "OK", d
            return "Sem CURDEPTH (talvez não seja QLOCAL?).", None
    return "Fila não encontrada.", None

# ===================== UI com filtro =====================
def construir_ui():
    import gradio as gr
    with gr.Blocks(title="Portal IBM MQ") as demo:
        gr.Markdown("# Portal IBM MQ")

        # Ambiente
        with gr.Row():
            env_dd = gr.Dropdown(label="Ambiente", choices=list(MQ_ENVS.keys()), value=DEFAULT_ENV, scale=1)

        # 1) Selecionar QMgr
        with gr.Row():
            qmgr_dd = gr.Dropdown(label="Queue Manager", choices=[], value=None, scale=2)
            btn_qmgr = gr.Button("Carregar QMgrs", scale=1)
        msg_qmgr = gr.Markdown("")

        def _load_qmgrs(env):
            nomes = listar_qmgrs(env)
            if not nomes:
                return gr.update(choices=[], value=None), "Falha ao listar QMgrs (verifique URL/credenciais)."
            return gr.update(choices=nomes, value=nomes[0]), "QMgrs carregados."

        btn_qmgr.click(_load_qmgrs, inputs=[env_dd], outputs=[qmgr_dd, msg_qmgr])
        demo.load(_load_qmgrs, inputs=[env_dd], outputs=[qmgr_dd, msg_qmgr])

        # 2) Listar filas + filtro
        with gr.Row():
            btn_listar = gr.Button("Listar filas (QLOCAL)")
        with gr.Row():
            filtro_tb = gr.Textbox(label="Filtrar filas (contém)", placeholder="Ex.: DEV. ou INPUT", scale=2)
            only_pos = gr.Checkbox(label="Somente profundidade > 0", value=False)
        filas_box = gr.Textbox(label="Filas", lines=14, show_copy_button=True)
        status_list = gr.Markdown("")
        filas_cache = gr.State([])  # armazena [(nome, depth)]

        def _format_pairs(pairs: List[tuple[str, Optional[int]]]) -> str:
            return "\n".join(f"{nm} ({d if isinstance(d,int) else 'NA'})" for nm, d in pairs)

        def _listar(env, q, only_positive=False):
            st, pairs, err = listar_filas_pairs(env, q)
            if only_positive:
                pairs = [(n, d) for n, d in pairs if isinstance(d, int) and d > 0]
            texto = _format_pairs(pairs) if pairs else (err or "(sem filas)")
            return texto, f"**Status:** {st}", pairs

        def _filtrar(filtro: str, only_positive: bool, pairs: List[tuple[str, Optional[int]]]):
            if not pairs:
                return "(Liste as filas primeiro)"
            f = (filtro or "").strip().lower()
            out = pairs
            if f:
                out = [(nm, d) for nm, d in out if f in nm.lower()]
            if only_positive:
                out = [(nm, d) for nm, d in out if isinstance(d, int) and d > 0]
            return _format_pairs(out) if out else "(nenhum resultado)"

        btn_listar.click(_listar, inputs=[env_dd, qmgr_dd, only_pos], outputs=[filas_box, status_list, filas_cache])
        filtro_tb.change(_filtrar, inputs=[filtro_tb, only_pos, filas_cache], outputs=[filas_box])
        only_pos.change(_filtrar, inputs=[filtro_tb, only_pos, filas_cache], outputs=[filas_box])

        # reset filtro/lista ao trocar QMgr ou Ambiente
        def _reset_on_change(_):
            return gr.update(value=""), [], gr.update(value=""), "—"
        qmgr_dd.change(_reset_on_change, inputs=[qmgr_dd], outputs=[filtro_tb, filas_cache, filas_box, status_list])
        env_dd.change(_reset_on_change, inputs=[env_dd], outputs=[filtro_tb, filas_cache, filas_box, status_list])

        # 3) Profundidade
        with gr.Row():
            fila_tb = gr.Textbox(label="Nome da fila", placeholder="Ex.: DEV.INPUT")
            btn_depth = gr.Button("Consultar profundidade")
        with gr.Row():
            status_out = gr.Textbox(label="Status", interactive=False)
            depth_out  = gr.Number(label="Profundidade", precision=0, interactive=False)

        btn_depth.click(lambda env, q, f: consultar_profundidade(env, q, f),
                        inputs=[env_dd, qmgr_dd, fila_tb],
                        outputs=[status_out, depth_out])

        # Rodapé dinâmico
        footer = gr.Markdown("")
        def _footer(env):
            c = MQ_ENVS.get(env)
            if not c: return "<small>Ambiente inválido</small>"
            return f"<small>Base: <code>{c.url_base}</code> · SSL verificado: <code>{bool(c.session.verify)}</code></small>"
        env_dd.change(_footer, inputs=[env_dd], outputs=[footer])
        demo.load(_footer, inputs=[env_dd], outputs=[footer])

    return demo

def main():
    ui = construir_ui()
    ui.launch()

if __name__ == "__main__":
    main()
