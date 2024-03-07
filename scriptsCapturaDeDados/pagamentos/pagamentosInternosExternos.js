const mysql = require("mysql2/promise");
const moment = require("moment");

const dbConfig = require("../../configuracoesBancoDeDados/configBancoDeDados");
const apitoken = require("../../../../informacoesAPI/nexo");

// FUNÇÃO PARA RETORNAR O ANO E O MÊS ATUAL
function anoMesAtual() {
  const dataAtual = new Date();
  const ano = dataAtual.getFullYear();
  const mes = dataAtual.getMonth() + 1;
  return { ano, mes };
}
const { ano, mes } = anoMesAtual();

// FUNÇÃO RESPONSÁVEL POR CAPTURAR OS DADOS DO NIBO
async function buscarDados() {
  const apiUrl = `https://api.nibo.com.br/empresas/v1/payments?$filter=year(date) eq ${ano} and month(date) eq ${mes}&apitoken=${apitoken}`;
  // const apiUrl = `https://api.nibo.com.br/empresas/v1/payments?apitoken=${apitoken}`

  try {
    const data = await (await fetch(apiUrl)).json();
    return data.items || [];
  } catch (error) {
    console.error("Erro ao buscar dados da API:", error);
    return [];
  }
}

// FORMATAR DATA PARA O MYSQL
function formatarDataParaMySQL(data) {
  return moment(data).format("YYYY-MM-DD HH:mm:ss");
}

// FUNÇÃO RESPONSÁVEL POR ARMAZENAR OS DADOS NO BANCO DE DADOS
async function inserirDados(data) {
  const connection = await mysql.createConnection(dbConfig);
  let [atualizadas, inseridas, atualizadas1, inseridas1] = [0, 0, 0, 0];

  try {
    for (const item of data) {
      if (item.isTransfer === true) {
        const [existe] = await connection.execute(
          "SELECT * FROM internalPayments WHERE entryId = ?",
          [item.entryId]
        );

        const query =
          existe.length > 0
            ? `UPDATE internalPayments SET bankBalanceDateIsGreaterThanEntryDate = ?, isVirtual = ?, accountId = ?, accountName = ?, accountIsDeleted = ?, date = ?, identifier = ?, value = ?, isReconciliated = ?,
              isTransfer = ?, isFlagged = ? 
                 WHERE entryId = ?`
            : `INSERT INTO internalPayments (entryId, bankBalanceDateIsGreaterThanEntryDate, isVirtual, accountId, accountName, accountIsDeleted, date, identifier, value, isReconciliated, isTransfer, isFlagged) 
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

        const params =
          existe.length > 0
            ? [
                item.bankBalanceDateIsGreaterThanEntryDate,
                item.isVirtual,
                item.account.id || null,
                item.account.name || null,
                item.account.isDeleted,
                formatarDataParaMySQL(item.date) || null,
                item.identifier || null,
                item.value || null,
                item.isReconciliated,
                item.isTransfer,
                item.isFlagged,
                item.entryId || null,
              ]
            : [
                item.entryId,
                item.bankBalanceDateIsGreaterThanEntryDate,
                item.isVirtual,
                item.account.id,
                item.account.name,
                item.account.isDeleted,
                formatarDataParaMySQL(item.date) || null,
                item.identifier,
                item.value,
                item.isReconciliated,
                item.isTransfer,
                item.isFlagged,
              ];

        const [result] = await connection.execute(query, params);

        existe.length > 0
          ? (atualizadas += result.changedRows)
          : (inseridas += result.affectedRows);
      } else {
        const [existe1] = await connection.execute(
          "SELECT * FROM externalPayments WHERE entryId = ?",
          [item.entryId]
        );
        const query =
          existe1.length > 0
            ? `UPDATE externalPayments SET bankBalanceDateIsGreaterThanEntryDate = ?,
        scheduleId = ?, isVirtual = ?, accountid = ?, accountname = ?, accountisdeleted = ?, stakeholderid = ?,
        stakeholderName = ?, stakeholderIsDeleted = ?, categoryId = ?, categoryName = ?, categoryIsDeleted = ?, categoryType = ?, categoryParentId = ?, categoryParentName = ?,
        date = ?, identifier = ?, value = ?, description = ?, checkNumber = ?, isReconciliated = ?, isTransfer = ?, isFlagged = ?, costCenterId = ?, costCenterName = ?, costCenterPercent = ?, costCenterValue = ? where entryId = ?`
            : `INSERT INTO externalPayments (entryId, bankBalanceDateIsGreaterThanEntryDate,
              scheduleId, isVirtual, accountid, accountname, accountisdeleted, stakeholderid, stakeholderName, stakeholderIsDeleted, categoryId, categoryName, categoryIsDeleted, categoryType, categoryParentId, categoryParentName,
              date, identifier, value, description, checkNumber, isReconciliated, isTransfer, isFlagged, costCenterId, costCenterName, costCenterPercent, costCenterValue, negativo, status, recebidoPago) 
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

        const params =
          existe1.length > 0
            ? [
                item.bankBalanceDateIsGreaterThanEntryDate,
                item.scheduleId || null,
                item.isVirtual,
                item.account.id || null,
                item.account.name || null,
                item.account.isDeleted,
                item.stakeholder.id || null,
                item.stakeholder.name || null,
                item.stakeholder.isDeleted,
                item.category.id || null,
                item.category.name || null,
                item.category.isDeleted,
                item.category.type || null,
                item.categories[0].parentId || null,
                item.categories[0].parent || null,
                formatarDataParaMySQL(item.date) || null,
                item.identifier || null,
                item.value || null,
                item.description || null,
                item.checkNum || null,
                item.isReconciliated || null,
                item.isTransfer,
                item.isFlagged,
                item.costCenter?.costCenterId || null,
                item.costCenter?.costCenterDescription || null,
                item.costCenters[0]?.percent || null,
                item.costCenters[0]?.value || null,
                item.entryId,
              ]
            : [
                item.entryId || null,
                item.bankBalanceDateIsGreaterThanEntryDate || null,
                item.scheduleId || null,
                item.isVirtual || null,
                item.account.id || null,
                item.account.name || null,
                item.account.isDeleted || null,
                item.stakeholder.id || null,
                item.stakeholder.name || null,
                item.stakeholder.isDeleted || null,
                item.category.id || null,
                item.category.name || null,
                item.category.isDeleted || null,
                item.category.type || null,
                item.categories[0].parentId || null,
                item.categories[0].parent || null,
                formatarDataParaMySQL(item.date) || null,
                item.identifier || null,
                item.value || null,
                item.description || null,
                item.checkNum || null,
                item.isReconciliated || null,
                item.isTransfer || null,
                item.isFlagged || null,
                item.costCenter?.costCenterId || null,
                item.costCenter?.costCenterDescription || null,
                item.costCenters[0]?.percent || null,
                item.costCenters[0]?.value || null,
                null,
                null,
                null,
              ];

        const [result] = await connection.execute(query, params);

        existe1.length > 0
          ? (atualizadas1 += result.changedRows)
          : (inseridas1 += result.affectedRows);
      }
    }
    console.log("\n-- Pagamentos Internos --");
    console.log(
      `${atualizadas} consultas atualizadas no banco de dados.\n${inseridas} consultas inseridas no banco de dados.`
    );
    console.log("\n-- Pagamentos Externos --");
    console.log(
      `${atualizadas1} consultas atualizadas no banco de dados.\n${inseridas1} consultas inseridas no banco de dados.`
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
  let excluidas = 0;

  try {
    // Busque todos os registros no banco de dados
    const [dbData] = await connection.execute(
      "SELECT entryId FROM internalPayments"
    );

    // Crie um conjunto (Set) com os entry_ids dos registros no banco de dados
    const dbDataEntryIds = new Set(dbData.map((item) => item.entryId));

    // Crie um conjunto (Set) com os entry_ids dos registros da API
    const apiDataEntryIds = new Set(data.map((item) => item.entryId));

    // Encontre os entry_ids que estão no banco de dados, mas não na API
    const entryIdsToDelete = [...dbDataEntryIds].filter(
      (entryId) => !apiDataEntryIds.has(entryId)
    );

    // Deletar os registros que não existem mais na API
    for (const entryIdToDelete of entryIdsToDelete) {
      await connection.execute(
        "DELETE FROM internalPayments WHERE entryId = ?",
        [entryIdToDelete]
      );
      excluidas++;
    }

    console.log(`${excluidas} => Pagamentos excluidos !`);
  } catch (error) {
    console.error("Erro ao deletar dados do banco de dados:", error);
  } finally {
    connection.end(); // Feche a conexão com o banco de dados
  }
}

async function inserirNegativoPositivo() {
  const connection = await mysql.createConnection(dbConfig);

  try {
    await connection.execute("UPDATE externalPayments SET negativo = ?", [
      true,
    ]);
  } catch (error) {
    console.error('Erro ao inserir valor "false" na coluna "negativo":', error);
  } finally {
    connection.end(); // Feche a conexão com o banco de dados
  }
}

async function inserirRealizados() {
  const connection = await mysql.createConnection(dbConfig);

  try {
    // Atualiza o status com base na diferença entre openValue e paidValue
    const query = `
      UPDATE externalPayments
      SET status = 'Realizados'
    `;
    await connection.execute(query);
  } catch (error) {
    console.error('Erro ao inserir valor na coluna "status":', error);
  } finally {
    connection.end(); // Fecha a conexão com o banco de dados
  }
}

async function inserirPagamentos() {
  const connection = await mysql.createConnection(dbConfig);

  try {
    // Atualiza o status com base na diferença entre openValue e paidValue
    const query = `
      UPDATE externalPayments
      SET recebidoPago = 'Pagamentos'
    `;
    await connection.execute(query);
  } catch (error) {
    console.error('Erro ao inserir valor na coluna "status":', error);
  } finally {
    connection.end(); // Fecha a conexão com o banco de dados
  }
}

// FUNÇÃO PRINCIPAL
(async () => {
  try {
    const dadosDaAPI = await buscarDados();
    if (dadosDaAPI.length > 0) {
      await inserirDados(dadosDaAPI);
      await inserirNegativoPositivo();
      await inserirRealizados();
      await inserirPagamentos();
      // await deletarDados(dadosDaAPI);
    }
  } catch (error) {
    console.error("Erro no processo:", error);
  }
})();
