/**
 * Underwriting API service
 */

import { API_BASE } from '../config.js';
import { postFormData } from '../api.js';

export async function extractApplication(applicationFile) {
  const form = new FormData();
  form.append("application", applicationFile);
  return await postFormData('/api/underwrite/extract-application', form);
}

export async function analyzeStatementsAndMatch(statements, state, applicationData, existingStatements = null) {
  const form = new FormData();
  statements.forEach(f => form.append("statements", f));
  form.append("state", state);
  form.append("application_json", JSON.stringify(applicationData));

  if (existingStatements) {
    form.append("existing_statements_json", JSON.stringify(existingStatements));
  }

  return await postFormData('/api/underwrite/statements-and-match', form);
}

export async function sendEmails(payload) {
  return await postFormData('/api/underwrite/send-emails',
    Object.keys(payload).reduce((fd, key) => {
      fd.append(key, typeof payload[key] === 'object' ? JSON.stringify(payload[key]) : payload[key]);
      return fd;
    }, new FormData())
  );
}
