// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0. See LICENSE at the package root.

import eslint from '@eslint/js';
import tsdoc from 'eslint-plugin-tsdoc';
import tseslint from 'typescript-eslint';

const licenseHeader = {
  meta: {
    type: 'problem',
    schema: [],
    messages: { missing: 'Start this file with the Apache-2.0 SPDX header, after any shebang.' },
  },
  create(context) {
    return {
      Program(node) {
        const lines = context.sourceCode.lines;
        const first = lines[0]?.startsWith('#!') ? 1 : 0;
        if (lines[first] !== '// SPDX-License-Identifier: Apache-2.0') {
          context.report({ node, messageId: 'missing' });
        }
      },
    };
  },
};

export default tseslint.config(
  { ignores: ['**/dist/**', '**/coverage/**', '**/node_modules/**', 'tmp/**'] },
  eslint.configs.recommended,
  tseslint.configs.recommended,
  {
    files: ['**/*.ts', '**/*.mjs'],
    plugins: { project: { rules: { 'license-header': licenseHeader } } },
    rules: { 'project/license-header': 'error' },
  },
  {
    files: ['**/*.ts'],
    plugins: { tsdoc },
    rules: {
      curly: ['error', 'all'],
      '@typescript-eslint/no-explicit-any': 'error',
      '@typescript-eslint/no-unused-vars': ['error', { argsIgnorePattern: '^_' }],
      'tsdoc/syntax': 'error',
    },
  },
  {
    files: ['src/**/*.ts', 'example/src/**/*.ts'],
    languageOptions: {
      parserOptions: { projectService: true, tsconfigRootDir: import.meta.dirname },
    },
    rules: {
      '@typescript-eslint/await-thenable': 'error',
      '@typescript-eslint/no-floating-promises': 'error',
      '@typescript-eslint/no-misused-promises': 'error',
      '@typescript-eslint/only-throw-error': 'error',
      '@typescript-eslint/consistent-type-imports': ['error', { fixStyle: 'inline-type-imports' }],
      '@typescript-eslint/no-import-type-side-effects': 'error',
    },
  },
);
