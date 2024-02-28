const mysql = require("mysql2/promise");
const moment = require("moment");

const dbConfig = require("../../configuracoesBancoDeDados/configBancoDeDados");
const apitoken = require("../../../apitokens/configuracoesNexo/configAPI");

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
  const apiUrl = `https://api.nibo.com.br/empresas/v1/schedules/debit?$filter=year(accrualDate) eq ${ano} and month(accrualDate) eq ${mes}&apitoken=${apitoken}`;

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
  let [atualizadas, inseridas] = [0, 0];

  try {
    for (const item of data) {
      const [existe] = await connection.execute(
        "SELECT * FROM scheduledPayments WHERE scheduleId = ?",
        [item.scheduleId]
      );

      const query =
        existe.length > 0
          ? `UPDATE scheduledPayments SET categoryId = ?, categoryName = ?, categoryType = ?, categoryParentId = ?, categoryParentName = ?, costCenterId = ?, costCenterName = ?, costCenterPercent = ?
              , type = ?, isEntry = ?, isBill = ?, isDebitNote = ?, isFlagged = ?, isDued = ?, dueDate = ?, accrualDate = ?, scheduleDate = ?, deleteDate = ?, createDate = ?, value = ?, isPaid = ?,
               costCenterValueType = ?, paidValue = ?, openValue = ?, stakeholderType = ?, stakeholderId = ?, stakeholderName = ?, stakeholderIsDeleted = ?, description = ?, hasInstallment = ?,
                installmentId = ?, hasRecurrence = ?, hasOpenEntryPromise = ?, hasEntryPromise = ?, autoGenerateEntryPromise = ?, hasInvoice = ?,
              hasPendingInvoice = ?, hasScheduleInvoice = ?, autoGenerateNfseType = ?, isPaymentScheduled = ? where scheduleId = ?`
          : `INSERT INTO scheduledPayments (scheduleId, categoryId, categoryName, categoryType, categoryParentId, categoryParentName, costCenterId, costCenterName, costCenterPercent, type,
                  isEntry, isBill, isDebitNote, isFlagged, isDued, dueDate, accrualDate, scheduleDate, deleteDate, createDate, value, isPaid, costCenterValueType, paidValue, openValue, stakeholderType, stakeholderId, stakeholderName,
                  stakeholderIsDeleted, description, hasInstallment, installmentId, hasRecurrence, hasOpenEntryPromise, hasEntryPromise, autoGenerateEntryPromise, hasInvoice,
                  hasPendingInvoice, hasScheduleInvoice, autoGenerateNfseType, isPaymentScheduled) 
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`;

      const params =
        existe.length > 0
          ? [
              item.categories[0].categoryId,
              item.categories[0].categoryName,
              item.categories[0].type,
              item.categories[0].parentId,
              item.categories[0].parent,
              item.costCenters[0]?.costCenterId ?? null,
              item.costCenters[0]?.costCenterDescription ?? null,
              item.costCenters[0]?.percent ?? null,
              item.type || null,
              item.isEntry || null,
              item.isBill || null,
              item.isDebitNote || null,
              item.isFlagged || null,
              item.isDued || null,
              formatarDataParaMySQL(item.dueDate) || null,
              formatarDataParaMySQL(item.accrualDate) || null,
              formatarDataParaMySQL(item.scheduleDate) || null,
              item.deleteDate ? formatarDataParaMySQL(item.deleteDate) : null,
              formatarDataParaMySQL(item.createDate) || null,
              item.value || null,
              item.isPaid || null,
              item.costCenterValueType || null,
              item.paidValue || null,
              item.openValue || null,
              item.stakeholder.type || null,
              item.stakeholder.id || null,
              item.stakeholder.name || null,
              item.stakeholder.isDeleted || null,
              item.description || null,
              item.hasInstallment || null,
              item.installmentId || null,
              item.hasRecurrence || null,
              item.hasOpenEntryPromise || null,
              item.hasEntryPromise || null,
              item.autoGenerateEntryPromise || null,
              item.hasInvoice || null,
              item.hasPendingInvoice || null,
              item.hasScheduleInvoice || null,
              item.autoGenerateNFSeType || null,
              item.isPaymentScheduled || null,
              item.scheduleId || null,
            ]
          : [
              item.scheduleId || null,
              item.categories[0].categoryId,
              item.categories[0].categoryName,
              item.categories[0].type,
              item.categories[0].parentId,
              item.categories[0].parent,
              item.costCenters[0]?.costCenterId ?? null,
              item.costCenters[0]?.costCenterDescription ?? null,
              item.costCenters[0]?.percent ?? null,
              item.type || null,
              item.isEntry || null,
              item.isBill || null,
              item.isDebitNote || null,
              item.isFlagged || null,
              item.isDued || null,
              formatarDataParaMySQL(item.dueDate) || null,
              formatarDataParaMySQL(item.accrualDate) || null,
              formatarDataParaMySQL(item.scheduleDate) || null,
              item.deleteDate ? formatarDataParaMySQL(item.deleteDate) : null,
              formatarDataParaMySQL(item.createDate) || null,
              item.value || null,
              item.isPaid || null,
              item.costCenterValueType || null,
              item.paidValue || null,
              item.openValue || null,
              item.stakeholder.type || null,
              item.stakeholder.id || null,
              item.stakeholder.name || null,
              item.stakeholder.isDeleted || null,
              item.description || null,
              item.hasInstallment || null,
              item.installmentId || null,
              item.hasRecurrence || null,
              item.hasOpenEntryPromise || null,
              item.hasEntryPromise || null,
              item.autoGenerateEntryPromise || null,
              item.hasInvoice || null,
              item.hasPendingInvoice || null,
              item.hasScheduleInvoice || null,
              item.autoGenerateNFSeType || null,
              item.isPaymentScheduled || null,
            ];

      const [result] = await connection.execute(query, params);

      existe.length > 0
        ? (atualizadas += result.changedRows)
        : (inseridas += result.affectedRows);
    }
    console.log("\n-- Agendados --")
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
  let excluidas = 0;

  try {
    const [result] = await connection.execute(
      "DELETE FROM scheduledPayments WHERE deleteDate IS NOT NULL"
    );

    excluidas = result.affectedRows;

    console.log("\n-- Agendados --")
    console.log(`${excluidas} pagamentos foram excluídos no NIBO !!`);
  } catch (error) {
    console.error("Erro ao excluir registros com deleteDate não nulo:", error);
  } finally {
    connection.end(); // A conexão é encerrada
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
