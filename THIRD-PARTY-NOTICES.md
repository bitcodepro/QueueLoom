# Third-party notices

QueueLoom is distributed under the [MIT License](LICENSE). Third-party components retain their own licenses; the QueueLoom license does not replace them.

This inventory was prepared from the restored NuGet dependency graph for the current `net10.0` projects. Regenerate it for every RID before distributing binaries because runtime and native assets can differ.

## Runtime and build dependencies

| Component | Version | License | Source/license |
|---|---:|---|---|
| Avalonia, Avalonia.Desktop, Avalonia.Themes.Fluent, Avalonia.Fonts.Inter, and Avalonia platform packages | 12.1.1 | MIT | [AvaloniaUI/Avalonia](https://github.com/AvaloniaUI/Avalonia), [license](https://github.com/AvaloniaUI/Avalonia/blob/main/licence.md) |
| Avalonia.BuildServices (build-time, transitive) | 11.3.2 | MIT | [AvaloniaUI/Avalonia.BuildServices](https://github.com/AvaloniaUI/Avalonia.BuildServices) |
| Azure.Identity | 1.21.0 | MIT | [Azure SDK for .NET — Identity](https://github.com/Azure/azure-sdk-for-net/tree/main/sdk/identity/Azure.Identity), [license](https://github.com/Azure/azure-sdk-for-net/blob/main/LICENSE.txt) |
| Azure.Messaging.ServiceBus | 7.20.2 | MIT | [Azure SDK for .NET — Service Bus](https://github.com/Azure/azure-sdk-for-net/tree/main/sdk/servicebus/Azure.Messaging.ServiceBus), [license](https://github.com/Azure/azure-sdk-for-net/blob/main/LICENSE.txt) |
| Azure.Core / Azure.Core.Amqp / System.ClientModel | 1.60.0 / 1.3.1 / 1.14.0 | MIT | [Azure SDK for .NET](https://github.com/Azure/azure-sdk-for-net), [license](https://github.com/Azure/azure-sdk-for-net/blob/main/LICENSE.txt) |
| Microsoft.Azure.Amqp | 2.7.0 | MIT | [Azure/azure-amqp](https://github.com/Azure/azure-amqp), [license](https://github.com/Azure/azure-amqp/blob/master/LICENSE) |
| Microsoft.Identity.Client / Extensions.Msal / IdentityModel.Abstractions | 4.84.2 / 4.84.2 / 8.14.0 | MIT | [AzureAD/microsoft-authentication-library-for-dotnet](https://github.com/AzureAD/microsoft-authentication-library-for-dotnet), [license](https://github.com/AzureAD/microsoft-authentication-library-for-dotnet/blob/main/LICENSE) |
| System.Security.Cryptography.ProtectedData | 10.0.11 | MIT | [dotnet/runtime](https://github.com/dotnet/runtime), [license](https://github.com/dotnet/runtime/blob/main/LICENSE.TXT) |
| Microsoft.Extensions.*, Microsoft.Bcl.AsyncInterfaces, System.Memory.Data | 10.0.9 | MIT | [dotnet/runtime](https://github.com/dotnet/runtime), [license](https://github.com/dotnet/runtime/blob/main/LICENSE.TXT) |
| SkiaSharp and native assets | 3.119.4 | MIT package; native third-party notices apply | [mono/SkiaSharp](https://github.com/mono/SkiaSharp), [license](https://github.com/mono/SkiaSharp/blob/main/LICENSE.md) |
| HarfBuzzSharp and native assets | 8.3.1.3 | MIT package; bundled HarfBuzz notice applies | [mono/SkiaSharp](https://github.com/mono/SkiaSharp), [HarfBuzz license](https://github.com/harfbuzz/harfbuzz/blob/main/COPYING) |
| MicroCom.Runtime | 0.11.6 | MIT | [kekekeks/MicroCom](https://github.com/kekekeks/MicroCom), [license](https://github.com/kekekeks/MicroCom/blob/master/LICENSE) |
| Tmds.DBus.Protocol | 0.94.1 | MIT | [tmds/Tmds.DBus](https://github.com/tmds/Tmds.DBus), [license](https://github.com/tmds/Tmds.DBus/blob/main/LICENSE) |
| Avalonia.Angle.Windows.Natives | 2.1.27548.20260419 | See bundled `LICENSE` | [NuGet package](https://www.nuget.org/packages/Avalonia.Angle.Windows.Natives/2.1.27548.20260419) |
| Inter font family, embedded by Avalonia.Fonts.Inter | bundled with 12.1.1 | SIL Open Font License 1.1 | [rsms/inter](https://github.com/rsms/inter), [OFL-1.1](https://github.com/rsms/inter/blob/master/LICENSE.txt) |

## Test-only dependencies

Test packages are used during development but are not included in the normal publish output.

| Component | Version | License | Source/license |
|---|---:|---|---|
| Microsoft.NET.Test.Sdk and Microsoft Test Platform packages | 18.8.1 | MIT | [microsoft/vstest](https://github.com/microsoft/vstest), [license](https://github.com/microsoft/vstest/blob/main/LICENSE) |
| coverlet.collector | 10.0.1 | MIT | [coverlet-coverage/coverlet](https://github.com/coverlet-coverage/coverlet), [license](https://github.com/coverlet-coverage/coverlet/blob/master/LICENSE) |
| xunit / xunit.analyzers | 2.9.3 / 1.18.0 | Apache-2.0 | [xunit/xunit](https://github.com/xunit/xunit), [license](https://github.com/xunit/xunit/blob/main/LICENSE) |
| xunit.runner.visualstudio | 3.1.5 | Apache-2.0 | [xunit/visualstudio.xunit](https://github.com/xunit/visualstudio.xunit), [license](https://github.com/xunit/visualstudio.xunit/blob/main/LICENSE) |
| Newtonsoft.Json (test transitive) | 13.0.3 | MIT | [JamesNK/Newtonsoft.Json](https://github.com/JamesNK/Newtonsoft.Json), [license](https://github.com/JamesNK/Newtonsoft.Json/blob/master/LICENSE.md) |

## MIT license text used by the listed MIT components

Principal attributions taken from NuGet metadata and upstream license files:

- Copyright 2013–2026 © The AvaloniaUI Project; Avalonia.BuildServices: Copyright 2023–2025 © The AvaloniaUI Project.
- Azure SDK, Microsoft identity, .NET, Test Platform, SkiaSharp, and related Microsoft packages: © Microsoft Corporation and their respective contributors.
- MicroCom.Runtime: Copyright 2021 © Nikita Tsukanov.
- Tmds.DBus.Protocol: Tom Deseyn and contributors.
- Inter font: Copyright (c) 2016 The Inter Project Authors; SIL OFL-1.1 applies, not MIT.
- xUnit.net: Copyright (C) .NET Foundation; Apache-2.0 applies, not MIT.

All other copyright notices belong to the respective authors and rights holders identified in the upstream repositories, NuGet package metadata, and bundled native notices.

```text
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

The complete Apache-2.0, SIL OFL, and native third-party license texts are available through the links above and must accompany the corresponding binary components when their licenses require it.

## Packaging requirements

- Do not remove license or notice files supplied by NuGet packages or native assets.
- For self-contained deployment, include the applicable [.NET runtime third-party notices](https://github.com/dotnet/runtime/blob/main/THIRD-PARTY-NOTICES.TXT).
- Generate the SBOM from the actual published directory for each RID, not only from the `.csproj` files.
- Whenever a package version changes, recheck its NuGet `license` metadata and upstream notice files.
- If an installer combines multiple architectures, use a merged dependency inventory covering every included artifact.

This file is an engineering inventory and does not replace legal review of a particular distribution method.
