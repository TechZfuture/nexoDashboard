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

async function buscarDados() {
//const apiUrl = `https://api.nibo.com.br/empresas/v1/receipts?apitoken=${apitoken}`;
  const apiUrl = `https://api.nibo.com.br/empresas/v1/receipts?$filter=year(date) eq ${ano} and month(date) eq ${mes}&apitoken=${apitoken}`;

  try {
    const data = await (await fetch(apiUrl)).json();
    return data.items || [];
  } catch (error) {
    console.error("Erro ao buscar dados da API:", error);
    return [];
  }
}

function formatarDataParaMySQL(data) {
  return moment(data).format("YYYY-MM-DD HH:mm:ss");
}

async function inserirDados(data) {
  const connection = await mysql.createConnection(dbConfig);
  let [atualizadas, inseridas, atualizadas1, inseridas1] = [0, 0, 0, 0];

  try {
    for (const item of data) {
      if (item.isTransfer === true) {
        const [existe] = await connection.execute(
          "SELECT * FROM internalIncomingBills WHERE entryId = ?",
          [item.entryId]
        );

        const query =
          existe.length > 0
            ? `UPDATE internalIncomingBills SET bankBalanceDateIsGreaterThanEntryDate = ?, isVirtual = ?, accountId = ?, accountName = ?,
          accountIsDeleted = ?, date = ?, identifier = ?, value = ?, checkNumber = ?, isReconciliated = ?, isTransfer = ?, isFlagged = ?
           WHERE entryId = ?`
            : `INSERT INTO internalIncomingBills (entryId, bankBalanceDateIsGreaterThanEntryDate, isVirtual, accountId, accountName, accountIsDeleted, date, identifier, value, checkNumber,
            isReconciliated, isTransfer, isFlagged) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

        const params =
          existe.length > 0
            ? [
                item.bankBalanceDateIsGreaterThanEntryDate,
                item.isVirtual,
                item.account.id,
                item.account.name,
                item.account.isDeleted,
                formatarDataParaMySQL(item.date) || null,
                item.identifier,
                item.value,
                item.checkNum || null,
                item.isReconciliated,
                item.isTransfer,
                item.isFlagged,
                item.entryId,
              ]
            : [
                item.entryId || null,
                item.bankBalanceDateIsGreaterThanEntryDate || null,
                item.isVirtual || null,
                item.account.id || null,
                item.account.name || null,
                item.account.isDeleted || null,
                formatarDataParaMySQL(item.date) || null,
                item.identifier || null,
                item.value || null,
                item.checkNum || null,
                item.isReconciliated || null,
                item.isTransfer || null,
                item.isFlagged || null,
              ];

        const [result] = await connection.execute(query, params);

        existe.length > 0
          ? (atualizadas += result.changedRows)
          : (inseridas += result.affectedRows);
      } else {
        const [existe1] = await connection.execute(
          "SELECT * FROM externalIncomingBills WHERE entryId = ?",
          [item.entryId]
        );

        const query =
          existe1.length > 0
            ? `UPDATE externalIncomingBills SET bankBalanceDateIsGreaterThanEntryDate = ?,
          scheduleId = ?, isVirtual = ?, accountId = ?, accountName = ?, accountIsDeleted = ?, stakeholderId = ?,
          stakeholderName = ?, stakeholderIsDeleted = ?, categoryId = ?, categoryName = ?, categoryIsDeleted = ?, categoryType = ?, categoryParentId = ?, categoryParentName = ?,
          date = ?, identifier = ?, value = ?, description = ?, checkNumber = ?, isReconciliated = ?, isTransfer = ?, isFlagged = ?, costCenterId = ?, costCenterName = ?, costCenterPercent = ?, costCenterValue = ?
             WHERE entryId = ?`
            : `INSERT INTO externalIncomingBills (entryId, bankBalanceDateIsGreaterThanEntryDate,
            scheduleId, isVirtual, accountId, accountName, accountIsDeleted, stakeholderId,
             stakeholderName, stakeholderIsDeleted, categoryId, categoryName, categoryIsDeleted, categoryType, categoryParentId, categoryParentName,
             date, identifier, value, description, checkNumber, isReconciliated, isTransfer, isFlagged, costCenterId, costCenterName, costCenterPercent, costCenterValue, status, negativo, recebidoPago) 
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
                item.costCenter?.costCenterId ?? null,
                item.costCenter?.costCenterDescription ?? null,
                item.costCenters[0]?.percent ?? null,
                item.costCenters[0]?.value ?? null,
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
                item.costCenter?.costCenterId ?? null,
                item.costCenter?.costCenterDescription ?? null,
                item.costCenters[0]?.percent ?? null,
                item.costCenters[0]?.value ?? null,
                null,
                null,
                null,
              ];

        const [result] = await connection.execute(query, params);

        existe1.length > 0
          ? (atualizadas1 += result.changedRows)
          : (inseridas1 += result.affetedRows);
      }
    }
    console.log("\n-- Recebimentos Internos --");
    console.log(
      `${atualizadas} - consultas atualizadas no banco de dados.\n${inseridas} - consultas inseridas no banco de dados.`
    );
    console.log("\n-- Recebimentos Externos --");
    console.log(
      `${atualizadas1} - consultas atualizadas no banco de dados.\n${inseridas1} - consultas inseridas no banco de dados.`
    );
  } catch (error) {
    console.error("Erro ao inserir dados no banco de dados:", error);
  } finally {
    connection.end(); // Feche a conexão com o banco de dados
  }
}

async function deletarDados(data) {
  const connection = await mysql.createConnection(dbConfig);
  let excluidas = 0;

  try {
    // Busque todos os registros no banco de dados
    const [dbData] = await connection.execute(
      "SELECT entryId FROM internalIncomingBills"
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
        "DELETE FROM internalIncomingBills WHERE entryId = ?",
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

async function inserirNegativoEmTodosElementos() {
  const connection = await mysql.createConnection(dbConfig);

  try {
    await connection.execute("UPDATE externalIncomingBills SET negativo = ?", [
      false,
    ]);
  } catch (error) {
    console.error('Erro ao inserir valor "false" na coluna "negativo":', error);
  } finally {
    connection.end(); // Feche a conexão com o banco de dados
  }
}

async function inserirColunaStatus() {
  const connection = await mysql.createConnection(dbConfig);

  try {
    // Atualiza o status com base na diferença entre openValue e paidValue
    const query = `
        UPDATE externalIncomingBills
        SET status = 'Realizados'
      `;
    await connection.execute(query);
  } catch (error) {
    console.error('Erro ao inserir valor na coluna "status":', error);
  } finally {
    connection.end(); // Fecha a conexão com o banco de dados
  }
}

async function inserirValorCorreto() {
  const connection = await mysql.createConnection(dbConfig);

  try {
    // Atualiza o status com base na diferença entre openValue e paidValue
    const query = `
        UPDATE externalIncomingBills
        SET valorCorreto = value
      `;
    await connection.execute(query);
  } catch (error) {
    console.error('Erro ao inserir valor na coluna "status":', error);
  } finally {
    connection.end(); // Fecha a conexão com o banco de dados
  }
}

async function inserirRecebimento() {
  const connection = await mysql.createConnection(dbConfig);

  try {
    // Atualiza o status com base na diferença entre openValue e paidValue
    const query = `
        UPDATE externalIncomingBills
        SET recebidoPago = 'Recebimento'
      `;
    await connection.execute(query);
  } catch (error) {
    console.error('Erro ao inserir valor na coluna "status":', error);
  } finally {
    connection.end(); // Fecha a conexão com o banco de dados
  }
}

(async () => {
  try {
    const dadosDaAPI = await buscarDados();
    if (dadosDaAPI.length > 0) {
      await inserirDados(dadosDaAPI);
    //   await deletarDados(dadosDaAPI);
      await inserirNegativoEmTodosElementos()
      await inserirColunaStatus()
      await inserirValorCorreto()
      await inserirRecebimento()
    }
  } catch (error) {
    console.error("Erro no processo:", error);
  }
})();
