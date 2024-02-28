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

async function buscarDados() {
  // const apiUrl = `https://api.nibo.com.br/empresas/v1/schedules/credit/opened?$filter=year(accrualDate) eq ${ano} and month(accrualDate) eq ${mes}&apitoken=${apitoken}`;
  const apiUrl = `https://api.nibo.com.br/empresas/v1/schedules/debit/dued?$filter=year(accrualDate) eq ${ano} and month(accrualDate) eq ${mes}&apitoken=${apitoken}`;

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
  let [atualizadas, inseridas] = [0, 0];

  try {
    for (const item of data) {
      const [existe] = await connection.execute(
        "SELECT * FROM pastDuePayments WHERE idPastDuePayment = ?",
        [item.categories[0].id]
      );

      const query =
        existe.length > 0
          ? `UPDATE pastDuePayments SET idCategory = ?, nameCategory = ?, typeCategory = ?, idParentCategory = ?, nameParentCategory = ?, idCostCenter = ?, percentCostCenter = ?, descriptionCostCenter = ?
          , costCenterIsDeleted = ?, idSchedule = ?, type = ?, isEntry = ?, isBill = ?, isDebitNote = ?, isFlagged = ?, isDued = ?, dueDate = ?, accrualDate = ?, scheduleDate = ?, createDate = ?, value = ?, isPaid = ?,
          costCenterValueType = ?, paidValue = ?, openValue = ?, idStakeholder = ?, nameStakeholder = ?, typeStakeholder = ?, stakeholderIsDeleted = ?, description = ?, reference = ?, hasInstallment = ?,
          idInstallment = ?, hasRecurrence = ?, hasOpenEntryPromise = ?, hasEntryPromise = ?, autoGenerateEntryPromise = ?, hasInvoice = ?,
          hasPendingInvoice = ?, hasScheduleInvoice = ?, autoGenerateNfseType = ?, isPaymentScheduled = ? where idPastDuePayment = ?`
          : `INSERT INTO pastDuePayments (idPastDuePayment, idCategory, nameCategory, typeCategory, idParentCategory, nameParentCategory, idCostCenter, percentCostCenter, descriptionCostCenter, costCenterIsDeleted, idSchedule, type,
              isEntry, isBill, isDebitNote, isFlagged, isDued, dueDate, accrualDate, scheduleDate, createDate, value, isPaid, costCenterValueType, paidValue, openValue, idStakeholder, nameStakeholder,
              typeStakeholder, stakeholderIsDeleted, description, reference, hasInstallment, idInstallment, hasRecurrence, hasOpenEntryPromise, hasEntryPromise, autoGenerateEntryPromise, hasInvoice,
              hasPendingInvoice, hasScheduleInvoice, autoGenerateNfseType, isPaymentScheduled, status, valorCorreto) 
   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`;

      const params =
        existe.length > 0
          ? [
              item.categories[0].categoryId || null,
              item.categories[0].categoryName || null,
              item.categories[0].type || null,
              item.categories[0].parentId || null,
              item.categories[0].parent || null,
              item.costCenters[0]?.costCenterId ?? null,
              item.costCenters[0]?.percent ?? null,
              item.costCenters[0]?.costCenterDescription ?? null,
              item.costCenter?.isDeleted ?? null,
              item.scheduleId || null,
              item.type || null,
              item.isEntry || null,
              item.isBill || null,
              item.isDebitNote || null,
              item.isFlagged || null,
              item.isDued || null,
              formatarDataParaMySQL(item.dueDate) || null,
              formatarDataParaMySQL(item.accrualDate) || null,
              formatarDataParaMySQL(item.scheduleDate) || null,
              formatarDataParaMySQL(item.createDate) || null,
              item.value || null,
              item.isPaid || null,
              item.costCenterValueType || null,
              item.paidValue || null,
              item.openValue || null,
              item.stakeholder.id || null,
              item.stakeholder.name || null,
              item.stakeholder.type || null,
              item.stakeholder.isDeleted || null,
              item.description || null,
              item.reference || null,
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
              item.categories[0].id,
            ]
          : [
              item.categories[0].id,
              item.categories[0].categoryId || null,
              item.categories[0].categoryName || null,
              item.categories[0].type || null,
              item.categories[0].parentId || null,
              item.categories[0].parent || null,
              item.costCenters[0]?.costCenterId ?? null,
              item.costCenters[0]?.percent ?? null,
              item.costCenters[0]?.costCenterDescription ?? null,
              item.costCenter?.isDeleted ?? null,
              item.scheduleId || null,
              item.type || null,
              item.isEntry || null,
              item.isBill || null,
              item.isDebitNote || null,
              item.isFlagged || null,
              item.isDued || null,
              formatarDataParaMySQL(item.dueDate) || null,
              formatarDataParaMySQL(item.accrualDate) || null,
              formatarDataParaMySQL(item.scheduleDate) || null,
              formatarDataParaMySQL(item.createDate) || null,
              item.value || null,
              item.isPaid || null,
              item.costCenterValueType || null,
              item.paidValue || null,
              item.openValue || null,
              item.stakeholder.id || null,
              item.stakeholder.name || null,
              item.stakeholder.type || null,
              item.stakeholder.isDeleted || null,
              item.description || null,
              item.reference || null,
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
              null,
              null,
            ];

      const [result] = await connection.execute(query, params);

      existe.length > 0
        ? (atualizadas += result.changedRows)
        : (inseridas += result.affectedRows);
    }
    console.log("\n-- Pagamentos Vencidos --");
    console.log(
      `${atualizadas} consultas atualizadas no banco de dados.\n${inseridas} consultas inseridas no banco de dados.`
    );
  } catch (error) {
    console.error("Erro ao inserir dados no banco de dados:", error);
  } finally {
    connection.end(); // Feche a conexão com o banco de dados
  }
}

async function deletarDados(data) {
  const connection = await mysql.createConnection(dbConfig);

  try {
    const existingRecordIds = new Set(
      data.map((item) => item.categories[0].id)
    );
    const registrosNoBanco = (
      await connection.execute("SELECT idPastDuePayment FROM pastDuePayments")
    )[0];

    for (const { idPastDuePayment } of registrosNoBanco) {
      if (!existingRecordIds.has(idPastDuePayment)) {
        await connection.execute(
          "DELETE FROM pastDuePayments WHERE idPastDuePayment = ?",
          [idPastDuePayment]
        );
      }
    }
  } catch (error) {
    console.error("Erro ao verificar e excluir registros ausentes:", error);
  } finally {
    connection.end(); // Feche a conexão com o banco de dados
  }
}

async function inserirColunaStatus(data) {
  const connection = await mysql.createConnection(dbConfig);

  try {
    // Atualiza o status com base na diferença entre openValue e paidValue
    const query = `
        UPDATE pastDuePayments
        SET status = "A Realizar"
      `;
    await connection.execute(query);
  } catch (error) {
    console.error('Erro ao inserir valor na coluna "status":', error);
  } finally {
    connection.end(); // Fecha a conexão com o banco de dados
  }
}

async function inserirValorCorreto(data) {
  const connection = await mysql.createConnection(dbConfig);

  try {
    // Atualiza o status com base na diferença entre openValue e paidValue
    const query = `
        UPDATE pastDuePayments
        SET valorCorreto = CASE
        WHEN paidValue IS NOT NULL THEN value + paidValue
        ELSE value
        END;
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
      await inserirColunaStatus();
      await inserirValorCorreto();
    }
  } catch (error) {
    console.error("Erro no processo:", error);
  }
})();
