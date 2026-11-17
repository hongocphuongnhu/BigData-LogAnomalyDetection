# Mo file .docx bang Word de cap nhat muc luc, danh muc bang/hinh (docx-js khong tinh duoc so trang),
# luu lai va xuat PDF ben canh. Can Microsoft Word tren Windows.
# Dung: powershell -File scripts/update_docx_fields.ps1 doc/BaoCao_Nhom.docx
param([Parameter(Mandatory = $true)][string]$Path)

$full = (Resolve-Path $Path).Path
$pdf = [System.IO.Path]::ChangeExtension($full, '.pdf')
$word = New-Object -ComObject Word.Application
$word.Visible = $false
$word.DisplayAlerts = 0
try {
    $doc = $word.Documents.Open($full)
    # Cap nhat hai luot: luot dau chen so trang lam muc luc dai ra, luot sau sua lai so trang bi lech.
    for ($pass = 0; $pass -lt 2; $pass++) {
        $doc.Fields.Update() | Out-Null
        foreach ($toc in $doc.TablesOfContents) { $toc.Update() }
    }
    $doc.Save()
    $doc.ExportAsFixedFormat($pdf, 17)
    $doc.Close($false)
    Write-Output "Da cap nhat field: $full"
    Write-Output "Da xuat PDF: $pdf"
} finally {
    $word.Quit()
}
