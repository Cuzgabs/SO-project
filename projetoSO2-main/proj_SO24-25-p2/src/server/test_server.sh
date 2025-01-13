#!/bin/bash
set -x  # debug opcional

echo "=== Limpando FIFOs antigos..."
rm -f /tmp/register_fifo /tmp/client_req* /tmp/client_resp*

echo "=== Iniciando servidor..."
./server ./jobs_dir 5 1 /tmp/register_fifo &
SERVER_PID=$!
# Desanexa o servidor da lista de jobs do shell:
disown $SERVER_PID
sleep 1

# Verifica se o FIFO de registro foi criado
if [ ! -p /tmp/register_fifo ]; then
    echo "FAIL: Servidor não criou /tmp/register_fifo"
    kill "$SERVER_PID" 2>/dev/null
    exit 1
fi
echo "=== Servidor iniciado com sucesso (PID=$SERVER_PID)"

# -----------------------------------------
# Função para cada "cliente"
# -----------------------------------------
function run_client() {
    local CLIENT_ID="$1"

    local REQ_FIFO="/tmp/client_req${CLIENT_ID}"
    local RESP_FIFO="/tmp/client_resp${CLIENT_ID}"

    echo ">>> Criando FIFOs do cliente $CLIENT_ID..."
    mkfifo "$REQ_FIFO" "$RESP_FIFO"

    # Registra o cliente no servidor
    echo ">>> Registrando cliente $CLIENT_ID no servidor..."
    echo "${REQ_FIFO};${RESP_FIFO}" > /tmp/register_fifo
    sleep 0.2

    # cat das respostas
    cat "$RESP_FIFO" | while read -r line; do
        echo "[cliente $CLIENT_ID resp]: $line"
    done &
    local CAT_PID=$!

    echo "SUBSCRIBE foo"      > "$REQ_FIFO"
    sleep 1
    echo "PUBLISH foo HelloFromClient${CLIENT_ID}" > "$REQ_FIFO"
    sleep 1
    echo "UNSUBSCRIBE foo"   > "$REQ_FIFO"
    sleep 1
    echo "DISCONNECT"        > "$REQ_FIFO"
    sleep 1

    # Fecha a escrita
    exec 3>"$REQ_FIFO"
    exec 3>&-
    sleep 1

    echo "Matando cat do cliente $CLIENT_ID (PID=$CAT_PID)"
    kill "$CAT_PID" 2>/dev/null

    rm -f "$REQ_FIFO" "$RESP_FIFO"

    echo ">>> Cliente $CLIENT_ID finalizado."
}

# Lança vários clientes em paralelo
NUM_CLIENTS=5
for i in $(seq 1 "$NUM_CLIENTS"); do
    run_client "$i" &
done

# Aguarda apenas os clientes (run_client) finalizarem
wait

# Agora mata o servidor
echo "Encerrando servidor com kill -9 (PID=$SERVER_PID)..."
kill -9 "$SERVER_PID" 2>/dev/null

# (Opcional) Mata quaisquer cat que possam ter sobrado
pkill -9 -f "cat /tmp/client_resp" 2>/dev/null

echo "=== Script chegou ao fim com sucesso ==="
