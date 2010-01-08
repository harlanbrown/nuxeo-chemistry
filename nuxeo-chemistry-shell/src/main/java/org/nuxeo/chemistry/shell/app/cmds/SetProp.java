/*
 * (C) Copyright 2006-2008 Nuxeo SAS (http://nuxeo.com/) and contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * Contributors:
 *     bstefanescu
 */
package org.nuxeo.chemistry.shell.app.cmds;

import java.util.List;

import org.apache.chemistry.CMISObject;
import org.nuxeo.chemistry.shell.Console;
import org.nuxeo.chemistry.shell.Context;
import org.nuxeo.chemistry.shell.app.ChemistryApp;
import org.nuxeo.chemistry.shell.app.ChemistryCommand;
import org.nuxeo.chemistry.shell.app.utils.SimplePropertyManager;
import org.nuxeo.chemistry.shell.command.Cmd;
import org.nuxeo.chemistry.shell.command.CommandLine;
import org.nuxeo.chemistry.shell.command.CommandParameter;

/**
 * @author <a href="mailto:bs@nuxeo.com">Bogdan Stefanescu</a>
 *
 */
@Cmd(syntax="setp|setProperty key value", synopsis="Set the value of a property on the current context object")
public class SetProp extends ChemistryCommand {

    @Override
    protected void execute(ChemistryApp app, CommandLine cmdLine)
            throws Exception {
        List<CommandParameter> args = cmdLine.getArguments();
        if (args.size() != 2) {
            Console.getDefault().error("Missing required arguments: key value");
        }

        Context ctx = app.getContext();
        CMISObject obj = ctx.as(CMISObject.class);
        if (obj != null) {
            new SimplePropertyManager(obj).setProperty(args.get(0).getValue(), args.get(1).getValue());
        }
    }

}