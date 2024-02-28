const mysql = require("mysql2/promise");

const dbConfig = require("../../configuracoesBancoDeDados/configBancoDeDados");
const apitoken = require("../../../apitokens/configuracoesNexo/configAPI");

// FUNÇÃO RESPONSÁVEL POR CAPTURAR OS DADOS DO NIBO
async function capturarDados() {
  const apiUrl = `https://api.nibo.com.br/empresas/v1/schedules/categories/groups?apitoken=${apitoken}`;

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
      const [existe] = await connection.execute(
        "SELECT * FROM parentcategory WHERE idParent = ?",
        [item.id]
      );

      const query =
        existe.length > 0
          ? "UPDATE parentcategory SET id = ?, name = ?, referenceCode = ? WHERE idParent = ?"
          : "INSERT INTO parentcategory (id, idParent, name, referenceCode) VALUES (?, ?, ?, ?)";

      const params =
        existe.length > 0
          ? [item.referenceCode, item.name, item.referenceCode, item.id]
          : [item.referenceCode, item.id, item.name, item.referenceCode];

      const [result] = await connection.execute(query, params);

      existe.length > 0
        ? (atualizadas += result.changedRows)
        : (inseridas += result.affectedRows);
    }
    console.log("\n-- Categoria PAI --")
    console.log(
      `${atualizadas} consultas atualizadas no banco de dados.\n${inseridas} consultas inseridas no banco de dados.`
    );
  } catch (error) {
    console.error("ERRO ! DETALHES DO ERRO => ", error);
  } finally {
    connection.end(); // Feche a conexão com o banco de dados
  }
}

// FUNÇÃO RESPONSÁVEL POR VERIFICAR SE O DADO QUE EXISTE NO BANCO CONDIZ COM O DADO RETORNADO DA API, CASO NÃO, ELE É DELETADO DO BANCO DE DADOS
async function deletarDados(data) {
  const connection = await mysql.createConnection(dbConfig);

  try {
    const idsNaAPI = new Set(data.map((item) => item.id));
    const registrosNoBanco = (
      await connection.execute("SELECT idParent FROM parentCategory")
    )[0];

    // Iterar sobre os registros do banco de dados e excluir se o ID não estiver na API
    for (const { idParent } of registrosNoBanco) {
      if (!idsNaAPI.has(idParent)) {
        await connection.execute(
          "DELETE FROM parentCategory WHERE idParent = ?",
          [idParent]
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
    const dadosDaAPI = await capturarDados();
    if (dadosDaAPI.length > 0) {
      await inserirDados(dadosDaAPI);
      await deletarDados(dadosDaAPI);
    }
  } catch (error) {
    console.error("Erro no processo:", error);
  }
})();
