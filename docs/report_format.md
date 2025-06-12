# Report Format

The generated report files are plain text but tables use a semicolon (`;`) as the single delimiter. This ensures:

- Every cell is clearly separated by one symbol.
- Ad names or other fields may contain spaces or commas without breaking the layout.
- The `.txt` file can be imported directly into Excel or Google Sheets.

Example header used in the Top Ads section (after entity columns):

```
Período;ROAS;Inversión;Compras;NCPA;CVR;AOV;Alcance;Impresiones;CTR
```

This delimiter is consistent across the "Top Ads", "Top AdSets" and "Top Campañas" tables generated in the bitácora reports. All three sections share the same metric columns (`ROAS`, `Inversión`, `Compras`, `NCPA`, `CVR`, `AOV`, `Alcance`, `Impresiones`, `CTR`) after the entity identifiers.
