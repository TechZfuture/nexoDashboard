const mysql = require("mysql2/promise");

const dbConfig = require("../../configuracoesBancoDeDados/configBancoDeDados");
const apitoken = require("../../../apitokens/configuracoesNexo/configAPI");

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
      const [existe] = await connection.execute(
        "SELECT * FROM childCategory WHERE idChild = ?",
        [item.id]
      );

      const query =
        existe.length > 0
          ? "UPDATE childCategory SET id = ?, name = ?, referenceCode = ?, type = ?, subgroupId = ?, subgroupName = ?, groupType = ?, referenceCodeKey = ? WHERE idChild = ?"
          : "INSERT INTO childCategory (id, idChild, name, referenceCode, type, subgroupId, subgroupName, groupType, referenceCodeKey, codDRE, tipoCategoria) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

      const params =
        existe.length > 0
          ? [
              item.order || null,
              item.name,
              item.referenceCode || null,
              item.type,
              item.subgroupId || null,
              item.subgroupName || null,
              item.groupType,
              item.group.id,
              item.id,
            ]
          : [
              item.order || null,
              item.id,
              item.name,
              item.referenceCode || null,
              item.type,
              item.subgroupId || null,
              item.subgroupName || null,
              item.groupType,
              item.group.id,
              null,
              null,
            ];

      const [result] = await connection.execute(query, params);

      existe.length > 0
        ? (atualizadas += result.changedRows)
        : (inseridas += result.affectedRows);
    }
    console.log("\n-- Categoria FILHO --");
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
    const idsNaAPI = new Set(data.map((item) => item.id));
    const registrosNoBanco = (
      await connection.execute("SELECT idChild FROM childCategory")
    )[0];

    // Iterar sobre os registros do banco de dados e excluir se o ID não estiver na API
    for (const { idChild } of registrosNoBanco) {
      if (!idsNaAPI.has(idChild)) {
        await connection.execute(
          "DELETE FROM childCategory WHERE idChild = ?",
          [idChild]
        );
        console.log(`Registro com idChild ${idChild.id} foi excluído.`);
      }
    }
  } catch (error) {
    console.error("Erro ao deletar dados no banco de dados:", error);
  } finally {
    connection.end();
  }
}

// FUNÇÃO RESPONSÁVEL POR ATUALIZAR A COLUNA TIPO CATEGORIA
async function atualizarTipoCategoria() {
  const connection = await mysql.createConnection(dbConfig);

  try {
    const [registros] = await connection.execute("SELECT * FROM childcategory");

    for (const registro of registros) {
      const novoTipoCategoria =
        registro.type === "in" ? 2 : registro.type === "out" ? 3 : null;

      if (novoTipoCategoria !== null) {
        await connection.execute(
          "UPDATE childcategory SET tipoCategoria = ? WHERE idChild = ?",
          [novoTipoCategoria, registro.idChild]
        );
      }
    }
  } catch (error) {
    console.error("Erro ao atualizar tipoCategoria:", error);
  } finally {
    connection.end();
  }
}

// ATUALIZAR COLUNA DO COD DRE
async function atualizarCodDRE() {
  const connection = await mysql.createConnection(dbConfig);

  try {
    // Mapeia os referenceCodeKey para os respectivos tipoCategoria
    const referenciaTipoCategoriaMap = {
      "63f89fb0-926e-48d2-bc5d-2de05d47dfb6": 1,
      "a650979e-907c-46ea-b7e2-bdb4a46a965b": 2,
      "d974c25d-7e69-4d86-824e-1c650e93b839": 4,
      "fdf0fbf0-4a58-47d6-b6fd-0a1a8cf257df": 6,
      "ac34f2c1-4ba5-4c89-aabc-c3242559a060": 7,
    };

    const [registros] = await connection.execute("SELECT * FROM childCategory");

    for (const registro of registros) {
      const novoTipoCategoria =
        referenciaTipoCategoriaMap[registro.referenceCodeKey];

      if (novoTipoCategoria !== undefined) {
        await connection.execute(
          "UPDATE childCategory SET codDRE = ? WHERE idChild = ?",
          [novoTipoCategoria, registro.idChild]
        );
      }
    }
  } catch (error) {
    console.error(
      "Erro ao atualizar tipoCategoria por referenceCodeKey:",
      error
    );
  } finally {
    connection.end();
  }
}

// FUNÇÃO PRINCIPAL
;(async () => {
    try {
      const dadosDaAPI = await buscarDados()
      if (dadosDaAPI.length > 0) {
        await inserirDados(dadosDaAPI)
        await deletarDados(dadosDaAPI)
        await atualizarTipoCategoria()
        await atualizarCodDRE()
      }
    } catch (error) {
      console.error('Erro no processo:', error)
    }
  })()