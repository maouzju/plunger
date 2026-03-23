Option Explicit

Dim shell, fso, scriptDir, target, launchers, i, launcher, checkCommand, runCommand

Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

scriptDir = fso.GetParentFolderName(WScript.ScriptFullName)
target = """" & fso.BuildPath(scriptDir, "run.pyw") & """"
launchers = Array("pythonw", "pyw", "py")

For i = 0 To UBound(launchers)
    launcher = launchers(i)
    checkCommand = "cmd /c where " & launcher & " >nul 2>nul"
    If shell.Run(checkCommand, 0, True) = 0 Then
        runCommand = launcher & " " & target
        shell.Run runCommand, 0, False
        WScript.Quit 0
    End If
Next

MsgBox "Unable to find pythonw/pyw/py. Please install Python or fix the Python launcher association.", vbExclamation, "Plunger"
