const mysql = require("mysql2/promise");

const dbConfig = require("../../configuracoesBancoDeDados/configBancoDeDados");
const apitoken = require("../../../../informacoesAPI/nexo");

// FUNÇÃO RESPONSÁVEL POR CAPTURAR OS DADOS DO NIBO
async function buscarDados() {
  const apiUrl = `https://api.nibo.com.br/empresas/v1/schedules/categories?apitoken=${apitoken}`;

  try {
    const data = await (await fetch(apiUrl)).json();
    return data.items || [];
  } catch (error) {
    console.error("Erro ao buscar dados da API:", error);
    return [];
  }
}

// FUNÇÃO RESPONSÁVEL POR ARMAZENAR OS DADOS NO BANCO DE DADOS
async function inserirDados(data) {
  const connection = await mysql.createConnection(dbConfig);
  let [atualizadas, inseridas] = [0, 0];

  try {
    for (const item of data) {
      if (item.subgroupId != null) {
        const [existe] = await connection.execute(
          "SELECT * FROM subcategory WHERE subgroupId = ?",
          [item.subgroupId || null]
        );

        const query =
          existe.length > 0
            ? "UPDATE subcategory SET name = ?, subgroupId = ? WHERE subgroupId = ?"
            : "INSERT INTO subcategory (name, subgroupId) VALUES (?, ?)";

        const params =
          existe.length > 0
            ? [
                item.subgroupName || null,
                item.subgroupId || null,
                item.subgroupId || null,
              ]
            : [item.subgroupName || null, item.subgroupId || null];

        const [result] = await connection.execute(query, params);

        existe.length > 0
          ? (atualizadas += result.changedRows)
          : (inseridas += result.affectedRows);
      }
    }
    console.log("\n-- Subgrupos --");
    console.log(
      `${atualizadas} consultas atualizadas no banco de dados.\n${inseridas} consultas inseridas no banco de dados.`
    );
  } catch (error) {
    console.error("Erro ao inserir dados no banco de dados:", error);
  } finally {
    connection.end(); // Feche a conexão com o banco de dados
  }
}

// FUNÇÃO RESPONSÁVEL POR VERIFICAR SE O DADO QUE EXISTE NO BANCO CONDIZ COM O DADO RETORNADO DA API, CASO NÃO, ELE É DELETADO DO BANCO DE DADOS
async function deletarDados(data) {
  const connection = await mysql.createConnection(dbConfig);

  try {
    const subgroupIdSet = new Set(data.map((item) => item.subgroupId));
    const registrosNoBanco = await connection.execute(
      "SELECT subgroupId FROM subcategory"
    );

    for (const { subgroupId } of registrosNoBanco) {
      if (!subgroupIdSet.has(subgroupId)) {
        await connection.execute(
          "DELETE FROM subcategory WHERE subgroupId = ?",
          [subgroupId]
        );
        console.log(
          `\nRegistro com subgroupId ${subgroupId.subgroupId} foi excluído.`
        );
      }
    }
  } catch (error) {
    console.error("Erro ao deletar dados no banco de dados:", error);
  } finally {
    connection.end();
  }
}

// FUNÇÃO PRINCIPAL
(async () => {
  try {
    const dadosDaAPI = await buscarDados();
    if (dadosDaAPI.length > 0) {
      await inserirDados(dadosDaAPI);
      await deletarDados(dadosDaAPI);
    }
  } catch (error) {
    console.error("Erro no processo:", error);
  }
})();
